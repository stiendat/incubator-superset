# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Utility functions used across Superset"""


import sys
import logging
import time
import urllib.request
from collections import namedtuple
from datetime import datetime, timedelta
from email.utils import make_msgid, parseaddr
from urllib.request import urlopen
from urllib.error import URLError  # pylint: disable=ungrouped-imports

import croniter
import simplejson as json
from dateutil.tz import tzlocal
from flask import render_template, Response, session, url_for
from flask_babel import gettext as __
from flask_login import login_user
from retry.api import retry_call
from selenium.common.exceptions import WebDriverException
from selenium.webdriver import chrome, firefox
from werkzeug.http import parse_cookie

# Superset framework imports
from superset import app, db, security_manager, cache
from superset.extensions import celery_app
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset.models.schedules import (
    EmailDeliveryType,
    get_scheduler_model,
    ScheduleType,
    SliceEmailReportFormat,
)
from superset.utils.core import get_email_address_list, send_email_smtp

# Sorting table
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from email.mime.image import MIMEImage
import os
import random
from matplotlib.dates import DateFormatter
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

# Globals
config = app.config
logger = logging.getLogger("tasks.email_reports")
logger.setLevel(logging.INFO)

# Time in seconds, we will wait for the page to load and render
PAGE_RENDER_WAIT = 30

# Get current datetime
# https://stackoverflow.com/questions/36500197/python-get-time-from-ntp-server


# def RequestTimefromNtp(addr=config["NTP_SERVER"]):
#     REF_TIME_1970 = 2208988800      # Reference time
#     client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     data = '\x1b' + 47 * '\0'
#     client.sendto(bytes(data, encoding='utf8'), (addr, 123))
#     data, address = client.recvfrom(1024)
#     if data:
#         t = struct.unpack('!12I', data)[10]
#         t -= REF_TIME_1970
#     return datetime.fromtimestamp(t)

# Moved to superset.utils.request_time_from_ntp
from superset.utils.request_time_from_ntp import RequestTimefromNtp

EmailContent = namedtuple("EmailContent", ["body", "data", "images"])

# email_content_count = 0
# for i in config['CUSTOM_EMAIL_CONTENT']:
#     email_content_count += 1
# ### TODO: lấy nội dung mail qua slice.id, dict.get(id) -> content
# if (email_content_count==0):
#     EMAIL_CONTENT = config['DEFAULT_EMAIL_CONTENT']
# else:
#     EMAIL_CONTENT = config['CUSTOM_EMAIL_CONTENT']

# # def _get_email_content(id):
# #     EMAIL_CONTENT = config['CUSTOM_EMAIL_CONTENT']
# #     return EMAIL_CONTENT.get(id)
# def _get_email_content():
#     # Parse string
#     #   Ex: Hello {{name}}! {{time}}
#     EMAIL_CONTENT = config['USER_EMAIL_CONTENT']
#     EMAIL_CONTENT.split('{{').split('}}')


def _get_recipients(schedule):
    bcc = config["EMAIL_REPORT_BCC_ADDRESS"]

    if schedule.deliver_as_group:
        to = schedule.recipients
        yield (to, bcc)
    else:
        for to in get_email_address_list(schedule.recipients):
            yield (to, bcc)


def _deliver_email(schedule, subject, email):
    for (to, bcc) in _get_recipients(schedule):
        send_email_smtp(
            to,
            subject,
            email.body,
            config,
            data=email.data,
            images=email.images,
            bcc=bcc,
            mime_subtype="related",
            dryrun=config["SCHEDULED_EMAIL_DEBUG_MODE"],
        )


def _generate_mail_content(schedule, screenshot, name, url):
    # Get current datetime
    now = RequestTimefromNtp()

    if schedule.delivery_type == EmailDeliveryType.attachment:
        images = None
        data = {"screenshot.png": screenshot}
        body = __(
            '<p>Ngày %(_time)s</p></br><b><a href="%(url)s">%(view_more)s</a></b><p></p>',
            _time=now.strftime('%d/%m/%Y'),
            name=name,
            url=url,
            view_more=config['EXPLORE_IN_SUPERSET'],
        )
    elif schedule.delivery_type == EmailDeliveryType.inline:
        # Get the domain from the 'From' address ..
        # and make a message id without the < > in the ends
        domain = parseaddr(config["SMTP_MAIL_FROM"])[1].split("@")[1]
        msgid = make_msgid(domain)[1:-1]

        images = {msgid: screenshot}
        data = None
        body = __(
            """
            <p>Ngày %(_time)s</p></br>
            <img src="cid:%(msgid)s">
            <b><a href="%(url)s">%(view_more)s</a></b><p></p>
            """,
            _time=now.strftime('%d/%m/%Y'),
            url=url,
            msgid=msgid,
            view_more=config['EXPLORE_IN_SUPERSET'],
        )

    return EmailContent(body, data, images)


def _get_auth_cookies():
    # Login with the user specified to get the reports
    with app.test_request_context():
        user = security_manager.find_user(config["EMAIL_REPORTS_USER"])
        login_user(user)

        # A mock response object to get the cookie information from
        response = Response()
        app.session_interface.save_session(app, session, response)

    cookies = []

    # Set the cookies in the driver
    for name, value in response.headers:
        if name.lower() == "set-cookie":
            cookie = parse_cookie(value)
            cookies.append(cookie["session"])

    return cookies


def _get_url_path(view, **kwargs):
    with app.test_request_context():
        return urllib.parse.urljoin(
            str(config["WEBDRIVER_BASEURL"]), url_for(view, **kwargs)
        )


def create_webdriver():
    # Create a webdriver for use in fetching reports
    if config["EMAIL_REPORTS_WEBDRIVER"] == "firefox":
        driver_class = firefox.webdriver.WebDriver
        options = firefox.options.Options()
    elif config["EMAIL_REPORTS_WEBDRIVER"] == "chrome":
        driver_class = chrome.webdriver.WebDriver
        options = chrome.options.Options()

    options.add_argument("--headless")

    # Prepare args for the webdriver init
    kwargs = dict(options=options)
    kwargs.update(config["WEBDRIVER_CONFIGURATION"])

    # Initialize the driver
    driver = driver_class(**kwargs)

    # Some webdrivers need an initial hit to the welcome URL
    # before we set the cookie
    welcome_url = _get_url_path("Superset.welcome")

    # Hit the welcome URL and check if we were asked to login
    driver.get(welcome_url)
    elements = driver.find_elements_by_id("loginbox")

    # This indicates that we were not prompted for a login box.
    if not elements:
        return driver

    # Set the cookies in the driver
    for cookie in _get_auth_cookies():
        info = dict(name="session", value=cookie)
        driver.add_cookie(info)

    return driver


def destroy_webdriver(driver):
    """
    Destroy a driver
    """

    # This is some very flaky code in selenium. Hence the retries
    # and catch-all exceptions
    try:
        retry_call(driver.close, tries=2)
    except Exception:  # pylint: disable=broad-except
        pass
    try:
        driver.quit()
    except Exception:  # pylint: disable=broad-except
        pass


def deliver_dashboard(schedule):
    """
    Given a schedule, delivery the dashboard as an email report
    """
    dashboard = schedule.dashboard

    dashboard_url = _get_url_path(
        "Superset.dashboard", dashboard_id=dashboard.id)

    # Create a driver, fetch the page, wait for the page to render
    driver = create_webdriver()
    window = config["WEBDRIVER_WINDOW"]["dashboard"]
    driver.set_window_size(*window)
    driver.get(dashboard_url)
    time.sleep(PAGE_RENDER_WAIT)

    # Set up a function to retry once for the element.
    # This is buggy in certain selenium versions with firefox driver
    get_element = getattr(driver, "find_element_by_class_name")
    element = retry_call(
        get_element, fargs=["grid-container"], tries=2, delay=PAGE_RENDER_WAIT
    )

    try:
        screenshot = element.screenshot_as_png
    except WebDriverException:
        # Some webdrivers do not support screenshots for elements.
        # In such cases, take a screenshot of the entire page.
        screenshot = driver.screenshot()  # pylint: disable=no-member
    finally:
        destroy_webdriver(driver)

    # Generate the email body and attachments
    email = _generate_mail_content(
        schedule, screenshot, dashboard.dashboard_title, dashboard_url
    )

    # Get current datetime
    now = RequestTimefromNtp()

    if (config['SHOW_TIME_ON_EMAIL_SUBJECT']):
        subject = __(
            "%(title)s (ngày %(_time)s)",
            title=dashboard.dashboard_title,
            _time=now.strftime('%d/%m/%Y')
        )
    else:
        subject = __(
            "%(title)s",
            title=dashboard.dashboard_title,
        )

    _deliver_email(schedule, subject, email)


def _get_slice_data(schedule):
    slc = schedule.slice

    slice_url = _get_url_path(
        "Superset.explore_json", csv="true", form_data=json.dumps({"slice_id": slc.id})
    )

    # URL to include in the email
    url = _get_url_path("Superset.slice", slice_id=slc.id)

    cookies = {}
    for cookie in _get_auth_cookies():
        cookies["session"] = cookie

    opener = urllib.request.build_opener()
    opener.addheaders.append(("Cookie", f"session={cookies['session']}"))
    response = opener.open(slice_url)
    if response.getcode() != 200:
        raise URLError(response.getcode())

    # Get current time
    now = RequestTimefromNtp()

    # TODO: Move to the csv module
    content = response.read()
    rows = [r.split(b",") for r in content.splitlines()]

    if schedule.delivery_type == EmailDeliveryType.inline:
        data = None

        # Parse the csv file and generate HTML
        columns = rows.pop(0)
        with app.app_context():
            body = render_template(
                "superset/reports/slice_data.html",
                columns=columns,
                rows=rows,
                name=slc.slice_name,
                link=url,
                view_more=config['EXPLORE_IN_SUPERSET'],

            )

    elif schedule.delivery_type == EmailDeliveryType.attachment:
        data = {__("%(name)s.csv", name=slc.slice_name): content}
        body = __(
            '<p>Ngày %(_time)s</p></br><b><a href="%(url)s">%(view_more)s</a></b><p></p>',
            _time=now.strftime('%d/%m/%Y'),
            name=slc.slice_name,
            url=url,
            view_more=config['EXPLORE_IN_SUPERSET'],
        )

    return EmailContent(body, data, None)

def _get_csv(slice_id):
    cache.clear()
    slice_url = _get_url_path(
        "Superset.explore_json", csv="true", form_data=json.dumps({"slice_id": slice_id})
    )
    #url = _get_url_path("Superset.slice", slice_id=slice_id)
    cookies = {}
    for cookie in _get_auth_cookies():
        cookies["session"] = cookie

    opener = urllib.request.build_opener()
    opener.addheaders.append(("Cookie", f"session={cookies['session']}"))
    response = opener.open(slice_url)
    if response.getcode() != 200:
        raise URLError(response.getcode())

    return response


def _get_raw_data(slice_id):
    response = _get_csv(slice_id)

    # TODO: Move to the csv module
    # content = response.read()
    df = pd.read_csv(response, header=0)
    pv = pd.pivot_table(df, index=['Ngày'], margins_name = "Tổng", aggfunc=np.sum, margins = True)
    pv = pv.reset_index()
    
    try:
        for row in df.index:
            temp = pv['Ngày'][row].split('-')
            pv.loc[row, 'Ngày'] = temp[2] + '-' + temp[1] + '-' + temp[0]
    except:
        pass
    # df = df.sort_values('Ngày', ascending=False)
    columns = [x for x in pv]
    # rows = [r.split(b",") for r in content.splitlines()]
    # columns = rows.pop(0)
    content_raw = """<table border='1' cellspacing='0' cellpadding='3'
      style='border: 1px solid #c0c0c0; border-collapse: collapse;'>
      <thead>
        <tr>
    """
    for column in columns:
        content_raw += "<th bgcolor='#92d050'>{0}</th>".format(
            column.replace('_', ' '))
    content_raw += "</tr> </thead><tbody>"
    for row in pv.index:
        content_raw += "<tr>"
        for column in pv:
            content_raw += """<td>{0}</td>""".format(pv[column][row])
        content_raw += "</tr>"
    content_raw += "</tbody></table>"

    return content_raw


def _get_slice_capture_old(slice_id):

    # Create a driver, fetch the page, wait for the page to render
    driver = create_webdriver()
    window = config["WEBDRIVER_WINDOW"]["slice"]
    driver.set_window_size(*window)

    slice_url = _get_url_path("Superset.slice", slice_id=slice_id)

    driver.get(slice_url)
    time.sleep(PAGE_RENDER_WAIT)

    # Set up a function to retry once for the element.
    # This is buggy in certain selenium versions with firefox driver
    element = retry_call(
        driver.find_element_by_class_name,
        fargs=["slice_container"],
        tries=2,
        delay=PAGE_RENDER_WAIT,
    )

    try:
        screenshot = element.screenshot_as_png
    except WebDriverException:
        # Some webdrivers do not support screenshots for elements.
        # In such cases, take a screenshot of the entire page.
        screenshot = driver.screenshot()  # pylint: disable=no-member
    finally:
        destroy_webdriver(driver)

    # Generate the email body and attachments
    return screenshot

def _get_slice_capture(slice_id):
    count_marker = 0
    count_color = 0
    response = _get_csv(slice_id)
    mydateparser = lambda x: pd.datetime.strptime(x, "%Y-%m-%d %H:%M:%S+00:00")
    df = pd.read_csv(response, parse_dates=['__timestamp'], date_parser=mydateparser)
    # ffff
    #df['__timestamp'] = pd.to_datetime(df["__timestamp"].dt.strftime('%d-%m-%Y'))
    df = df.set_index('__timestamp')
    df = df.sort_index()
    date_format = DateFormatter('%d-%m-%Y')
    fig, ax = plt.subplots(figsize=(15, 5))
    #plt.figure(figsize=(15, 5))
    plt.xticks(rotation=15)
    ax.xaxis.set_major_formatter(date_format)
    # plt.ylabel("asd")
    for i in df.columns:
        plt.plot(df[i], label=i, color=config['LINE_COLOR_LIST'][count_color])
        for idx,data in enumerate(df[i]):
            plt.text(df[i].index[idx], data, data, fontsize=12)
            try:
                plt.scatter(df[i].index[idx], data, marker=config['MARKER_LIST'][count_marker], color=config['LINE_COLOR_LIST'][count_color], s=50)
            except IndexError:
                raise IndexError('Current pos: \n i in df.cols: {}\n count_marker = {}\n marker = {}\n count_color = {}\n color = {}'.format(str(i), str(count_marker), str(config['MARKER_LIST']), str(count_color), str(config['LINE_COLOR_LIST'])))
            #plt.scatter(df[i].index[idx], data, marker='o', color='red', s=50)
        count_marker += 1
        if count_marker == len(config['MARKER_LIST']):
            count_marker = 0
        count_color += 1
        if count_color == len(config['LINE_COLOR_LIST']):
            count_color = 0
    plt.legend(loc='best')
    file_name = str(random.getrandbits(64))
    plt.savefig(config['EMAIL_CHART_PICTURE_CACHE_DIR'] + file_name + 'temp.png')
    # fp = open(config['EMAIL_CHART_PICTURE_CACHE_DIR'] + 'temp.png', 'rb')
    # img = fp.read()
    # fp.close()
    # os.remove(config['EMAIL_CHART_PICTURE_CACHE_DIR'] + 'temp.png', 'rb')
    return config['EMAIL_CHART_PICTURE_CACHE_DIR'] + file_name + 'temp.png'


def deliver_dashboard_v2(schedule):
    """
    Given a schedule, delivery the dashboard as an email report
    """

    # Get current time
    now = RequestTimefromNtp()

    dashboard = schedule.dashboard
    content = """<b>Dear các anh/chị,</b><p></p>
    Kính gửi anh/chị Báo cáo dịch vụ {} ngày {}<p></p>""".format(dashboard.dashboard_title.split('-')[1], now.strftime('%d/%m/%Y'))

    # TODO: Fix lay list slice id trong dashboard
    # slice_arr = db.session.query(Dashboard.id).charts()
    slice_arr = list()
    with urlopen('{0}dashboard/export_dashboards_form?id={1}&action=go'.format(config['WEBDRIVER_BASEURL'], dashboard.id)) as __dashboard__:
        dashboard_content = __dashboard__.read().decode()
    dashboard_content = json.loads(dashboard_content)
    for _item in dashboard_content['dashboards'][0]['__Dashboard__']['slices']:
        # name_arr = [tên dịch vụ, tên chart]
        name_arr = _item['__Slice__']['slice_name'].split('-')
        pos = name_arr[0].split('.')[0]
        name_arr = [name_arr[1][1:-1], name_arr[2][1:]]
        slice_arr.append((_item['__Slice__']['id'], _item['__Slice__']['viz_type'], name_arr, pos))
    slice_arr.sort(key=lambda x:int(x[3]))

    # Dicts chứa thông tin ảnh {cidID : sceenshot}
    images = dict()
    img_file_list = list()

    for slice_id in slice_arr:
        # db query not working !!!
        # type_slice = db.session.query(Slice.id).viz_type
        type_slice = slice_id[1]
        # Thêm tiêu đề của chart
        content += '</br><b>{}</b></br>'.format(str(slice_id[2][1]))
        if type_slice == 'pivot_table':

            content += _get_raw_data(slice_id[0])
        else:
            img_path = _get_slice_capture(slice_id[0])
            img = open(img_path, 'rb')
            img_file_list.append(img)
            images['{}'.format(slice_id[0])] = img.read()
            content += """<img src="cid:{0}" width="100%" height="auto">""".format(slice_id[0])
        content += '<p></p>'
    # Generate the email body and attachments
    content += "Best regards."
    email = EmailContent(content, None, images=images)
    if (config['SHOW_TIME_ON_EMAIL_SUBJECT']):
        subject = __(
            "%(title)s (ngày %(_time)s)",
            title='Báo cáo dịch vụ {}'.format(dashboard.dashboard_title.split('-')[1]),
            _time=now.strftime('%d/%m/%Y')
        )
    else:
        subject = __(
            "%(title)s",
            title='Báo cáo dịch vụ {}'.format(dashboard.dashboard_title.split('-')[1]),
        )

    _deliver_email(schedule, subject, email)
    for file_ in img_file_list:
        file_.close()


def _get_slice_visualization(schedule):
    slc = schedule.slice

    # Create a driver, fetch the page, wait for the page to render
    driver = create_webdriver()
    window = config["WEBDRIVER_WINDOW"]["slice"]
    driver.set_window_size(*window)

    slice_url = _get_url_path("Superset.slice", slice_id=slc.id)

    driver.get(slice_url)
    time.sleep(PAGE_RENDER_WAIT)

    # Set up a function to retry once for the element.
    # This is buggy in certain selenium versions with firefox driver
    element = retry_call(
        driver.find_element_by_class_name,
        fargs=["chart-container"],
        tries=2,
        delay=PAGE_RENDER_WAIT,
    )

    try:
        screenshot = element.screenshot_as_png
    except WebDriverException:
        # Some webdrivers do not support screenshots for elements.
        # In such cases, take a screenshot of the entire page.
        screenshot = driver.screenshot()  # pylint: disable=no-member
    finally:
        destroy_webdriver(driver)

    # Generate the email body and attachments
    return _generate_mail_content(schedule, screenshot, slc.slice_name, slice_url)


def deliver_slice(schedule):
    """
    Given a schedule, delivery the slice as an email report
    """
    if schedule.email_format == SliceEmailReportFormat.data:
        email = _get_slice_data(schedule)
    elif schedule.email_format == SliceEmailReportFormat.visualization:
        email = _get_slice_visualization(schedule)
    else:
        raise RuntimeError("Unknown email report format")

    # Get current datetime
    now = RequestTimefromNtp()

    if (config['SHOW_TIME_ON_EMAIL_SUBJECT']):
        subject = __(
            "%(title)s (ngày %(_time)s)",
            title=schedule.slice.slice_name,
            _time=now.strftime('%d/%m/%Y')
        )
    else:
        subject = __(
            "%(title)s",
            title=schedule.slice.slice_name,
        )

    _deliver_email(schedule, subject, email)


@celery_app.task(
    name="email_reports.send",
    bind=True,
    soft_time_limit=config["EMAIL_ASYNC_TIME_LIMIT_SEC"],
)
def schedule_email_report(
    task, report_type, schedule_id, recipients=None
):  # pylint: disable=unused-argument
    model_cls = get_scheduler_model(report_type)
    schedule = db.create_scoped_session().query(model_cls).get(schedule_id)

    # The user may have disabled the schedule. If so, ignore this
    if not schedule or not schedule.active:
        logger.info("Ignoring deactivated schedule")
        return

    # TODO: Detach the schedule object from the db session
    if recipients is not None:
        schedule.id = schedule_id
        schedule.recipients = recipients

    if report_type == ScheduleType.dashboard.value:
        # deliver_dashboard(schedule)
        deliver_dashboard_v2(schedule)
    elif report_type == ScheduleType.slice.value:
        deliver_slice(schedule)
    else:
        raise RuntimeError("Unknown report type")


def next_schedules(crontab, start_at, stop_at, resolution=0):
    crons = croniter.croniter(crontab, start_at - timedelta(seconds=1))
    previous = start_at - timedelta(days=1)

    for eta in crons.all_next(datetime):
        # Do not cross the time boundary
        if eta >= stop_at:
            break

        if eta < start_at:
            continue

        # Do not allow very frequent tasks
        if eta - previous < timedelta(seconds=resolution):
            continue

        yield eta
        previous = eta


def schedule_window(report_type, start_at, stop_at, resolution):
    """
    Find all active schedules and schedule celery tasks for
    each of them with a specific ETA (determined by parsing
    the cron schedule for the schedule)
    """
    model_cls = get_scheduler_model(report_type)
    dbsession = db.create_scoped_session()
    schedules = dbsession.query(model_cls).filter(model_cls.active.is_(True))

    for schedule in schedules:
        args = (report_type, schedule.id)

        # Schedule the job for the specified time window
        for eta in next_schedules(
            schedule.crontab, start_at, stop_at, resolution=resolution
        ):
            schedule_email_report.apply_async(args, eta=eta)


@celery_app.task(name="email_reports.schedule_hourly")
def schedule_hourly():
    """ Celery beat job meant to be invoked hourly """

    if not config["ENABLE_SCHEDULED_EMAIL_REPORTS"]:
        logger.info("Scheduled email reports not enabled in config")
        return

    resolution = config["EMAIL_REPORTS_CRON_RESOLUTION"] * 60

    # Get the top of the hour
    start_at = datetime.now(tzlocal()).replace(
        microsecond=0, second=0, minute=0)
    stop_at = start_at + timedelta(seconds=3600)
    schedule_window(ScheduleType.dashboard.value,
                    start_at, stop_at, resolution)
    schedule_window(ScheduleType.slice.value, start_at, stop_at, resolution)
