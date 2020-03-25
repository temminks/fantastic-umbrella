#!/usr/bin/env python
# coding: utf-8


import requests
from bs4 import BeautifulSoup, NavigableString
from bs4.element import Tag
from random import randint
from time import sleep
import re
from datetime import datetime, timedelta
from numpy import datetime64, timedelta64
from urllib.parse import unquote
import json
import glob
import pandas as pd
import asyncio
from aiohttp import ClientSession


headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate',
           'Upgrade-Insecure-Requests': '1',
           'Accept-Language': 'de;q=0.8',
          }

sess = requests.Session()
sess.headers.update(headers)


def grab(url: str, timeout=40):
    return BeautifulSoup(sess.get(url, timeout=timeout).text, 'lxml')


async def fetch(url, session):
    async with session.get(url) as response:
        resp = await response.text()
        return BeautifulSoup(resp, 'lxml')


async def run(urls):
    tasks = []
    async with ClientSession(headers=headers) as session:
        tasks = (asyncio.ensure_future(fetch(url, session)) for url in urls)
        return await asyncio.gather(*tasks)


class Course():
    """Provides basic functionality for specific course implementations."""
    def __init__(self, delayed=False):
        self.courses = set()
        if not delayed:
            self.courses = self.fetch_courses()

    @property
    def count(self):
        return len(self.courses)

    def __len__(self):
        return self.count()

    def __str__(self, source=None):
        return 'Found {} courses{}.'.format(self.count, bool(source) * ' on {}'.format(source))


class Freesamples(Course):
    def __init__(self, delayed=False):
        super().__init__(delayed)

    def fetch_courses(self):
        soup = grab('https://yofreesamples.com/courses/free-discounted-udemy-courses-list/')
        return set(course['href'] for course in soup.find_all('a', class_='course_title', href=True))

    def __str__(self):
        return super().__str__('Freesamples')


class Reddit(Course):
    """Reddit offers a json representation for their sites. We can search for courses in these postings."""
    def __init__(self, delayed=False):
        super().__init__(delayed)
    
    def fetch_courses(self):
        courses = set()
        regex = re.compile("http[s]*://www.udemy.com/course/[\w\d\-]{5,}/[\w?\d/]*couponCode=[\w]{3,}")

        page = sess.get('https://www.reddit.com/r/Udemy/.json?limit=100')
        for article in page.json()['data']['children']:
            if datetime.utcfromtimestamp(article['data']['created_utc']) > (datetime.now() - timedelta(10)):
                text = article['data']['selftext']
                urls = re.findall(regex, text)
                if urls:
                    courses = courses.union(urls)
        return courses
    
    def __str__(self):
        return super().__str__('Reddit')


class Dsmenders(Course):
    def __init__(self, delayed=False):
        super().__init__(delayed)
    
    def fetch_courses(self):
        dsmenders = set()
        for i in range(1, 6):
            soup = grab('https://tech.dsmenders.com/category/free-online-courses/page/{}/'.format(i), timeout=20)
            links = soup.find_all('h2', 'entry-title')
            dsmenders = dsmenders.union([link.find('a', href=True)['href'] for link in links])

        courses = set()
        for course in dsmenders:
            soup = grab(course, timeout=30)
            url = soup.find('a', href=True, target='_blank', class_=False, style=True)
            if url and url['href'].find('couponCode') > -1:
                courses.add(url['href'])
        return courses

    def __str__(self):
        return super().__str__('Dsmenders')


class Facebook(Course):
    """Facebook courses are published in groups."""
    def __init__(self, delayed=False):
        self.groups = [
            'https://www.facebook.com/groups/FreeUdemyCoursesOnline/',
            'https://www.facebook.com/groups/freeudemycouponscourses/',
            'https://www.facebook.com/groups/1602890986642463/',
            'https://www.facebook.com/groups/FreeUdemyCoursesOnline/',
            'https://www.facebook.com/Udemy.Bargains/',
            'https://www.facebook.com/FreeOnlineCoursesCoupon/',
            'https://www.facebook.com/groups/DiscountedUdemyCoursesOnline',
            'https://web.facebook.com/groups/677040975746787',
            'https://web.facebook.com/groups/eLearningTrainingCourses',
            'https://web.facebook.com/groups/OnlineCoursesUdemy/',
            'https://web.facebook.com/groups/427365844137526',
            'https://web.facebook.com/groups/BestUdemyCourses',
            'https://web.facebook.com/groups/freecoursesudemy',
            'https://web.facebook.com/groups/1858168261178187',
            'https://web.facebook.com/groups/freeanddiscountedudemycoursecoupons',
         ]
        super().__init__(delayed)

    def fetch_courses(self):
        facebook = set()
        regex = re.compile('https://www.udemy.com/course/[\w/?&-]+couponCode[=|%3D][\w-]{3,}[^=/&\"]')
        for group in self.groups:
            page = sess.get(group).text
            for link in re.findall(regex, page):
                facebook.add(unquote(link))
            sleep(randint(1,2))
        return facebook

    def __str__(self):
        return super().__str__('Facebook')


class UdemyParser():
    """Uses the Udemy API to find a course's meta data, parses it and creates a DataFrame."""
    def __init__(self, courses):
        self.df = pd.DataFrame(columns=['title', 'link', 'rating', 'num_ratings', 'language', 'duration',
                                        'topic_0', 'topic_1', 'topic_2', 'expiring', 'instructor'])
        self.api = 'https://www.udemy.com/api-2.0/courses/'
        self.params = '&fields[course]=is_paid,avg_rating,num_reviews,primary_category,content_info,discount,title,primary_subcategory,locale,visible_instructors'

        list_of_files = glob.glob('./udemy-courses/*.csv')
        self.day_before = pd.read_csv(list_of_files[-1])
        self.courses = courses

    async def parse(self):
        all_courses = set([course for course in self.courses if course.find('course/') > -1]) | set(self.day_before.link)
        urls = [self.api + course[course.find('course/')+7:] + self.params for course in all_courses]
        courses = await self.run(urls)

        for url, meta in zip(all_courses, courses):
            self.df = self.df.append(self.parse_course(url, meta), ignore_index=True)
            if not len(self)%5:
                print('parsed {} courses'.format(len(self)), end='\r')
        print('parsed {} courses'.format(len(self)), end='\r')

    @staticmethod
    def parse_language(lang):
        if lang == 'English (India)':
            return lang
        else:
            return lang.split()[0]

    def parse_course(self, url, meta):
        if meta.get('is_paid') and meta.get('discount') and meta['discount']['price']['amount'] == 0.0:
            expiration = meta['discount']['campaign']['end_time']
            return {'title': meta['title'],
                    'link': url,
                    'rating': meta['avg_rating'],
                    'num_ratings': meta['num_reviews'],
                    'language': self.parse_language(meta['locale']['title']),
                    'duration': meta['content_info'],
                    'expiring': timedelta64(datetime64(expiration)-datetime64('now'),'h'),
                    'topic_0': meta['primary_category']['title'],
                    'topic_1': meta['primary_subcategory']['title'],
                    'instructor': set([instructor['title'] for instructor in meta['visible_instructors']]),
                   }

    @staticmethod
    async def fetch(url, session):
        async with session.get(url) as response:
            return await response.json()

    async def run(self, urls):
        tasks = []
        async with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(self.fetch(url, session)) for url in urls)
            return await asyncio.gather(*tasks)

    def __len__(self):
        return len(self.df)
