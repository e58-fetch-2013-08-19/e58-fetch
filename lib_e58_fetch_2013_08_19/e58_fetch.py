# -*- mode: python; coding: utf-8 -*-
#
# Copyright 2013 Andrej Antonov <polymorphm@gmail.com>.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

assert str is not bytes

from urllib import parse as url
from urllib import request
import threading
import html5lib
from . import et_find

SITE_URL = 'http://e58.ru'
FETCH_TIMEOUT = 300.0
FETCH_MAX_LENGTH = 10000000

class FetchError(Exception):
    pass

class ParseError(Exception):
    pass

def fetch(opener, request):
    resp = opener.open(
            request,
            timeout=FETCH_TIMEOUT,
            )
    
    if resp.geturl() != request.get_full_url() or resp.getcode() != 200:
        raise FetchError('invalid url or invalid code')
    
    text = resp.read(FETCH_MAX_LENGTH).decode('utf-8', 'replace')
    
    return text

def data_fetch_thread(
        bulk_data_ctx,
        on_scheduled=None,
        on_begin=None,
        on_fetch=None,
        on_error=None,
        ):
    while True:
        with bulk_data_ctx.lock:
            try:
                fetch_url = bulk_data_ctx.url_list.pop(0)
            except IndexError:
                return
        
        try:
            if on_begin is not None:
                on_begin(fetch_url)
            
            opener = request.build_opener()
            
            html = fetch(opener, request.Request(fetch_url))
            doc = html5lib.parse(html)
            
            firm_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'attrib': {'id': 'firms'}},
                    {'tag': '{http://www.w3.org/1999/xhtml}h3'},
                    {'tag': '{http://www.w3.org/1999/xhtml}a'},
                    ))
            
            for firm_elem in firm_elem_list:
                firm_url = firm_elem.get('href')
                if firm_url is not None:
                    firm_url = url.urljoin(fetch_url, firm_url)
                
                with bulk_data_ctx.lock:
                    bulk_data_ctx.new_url_list.append(firm_url)
                
                if on_scheduled is not None:
                    on_scheduled(firm_url)
            
            paginator_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('paginator', )}},
                    {'tag': '{http://www.w3.org/1999/xhtml}a'},
                    ))
            
            for paginator_elem in paginator_elem_list:
                page_url = paginator_elem.get('href')
                if page_url is not None:
                    page_url = url.urljoin(fetch_url, page_url)
                
                with bulk_data_ctx.lock:
                    bulk_data_ctx.new_url_list.append(page_url)
                
                if on_scheduled is not None:
                    on_scheduled(page_url)
            
            title_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'tag': '{http://www.w3.org/1999/xhtml}h1'},
                    ))
            title = ' | '.join(' '.join(
                    frag.strip() for frag in elem.itertext()) for elem in title_elem_list
                    )
            if not title:
                continue
            fetch_data = {'title': title}
            
            director_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('director', )}},
                    ))
            fetch_data['director'] = ' | '.join(
                    ' '.join(frag.strip() for frag in elem.itertext()) for elem in director_elem_list
                    )
            
            uaddress_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('uaddress', )}},
                    ))
            fetch_data['uaddress'] = ' | '.join(filter(
                    None,
                    (elem.text for elem in uaddress_elem_list),
                    ))
            
            address_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('address', )}},
                    {'any': (
                            {'tag': '{http://www.w3.org/1999/xhtml}div', 'in_attrib': {'class': ('address', )}},
                            {'tag': '{http://www.w3.org/1999/xhtml}a', 'in_attrib': {'class': ('maplinked', )}},
                            )},
                    ))
            fetch_data['address'] = ' | '.join(filter(
                    None,
                    (elem.text for elem in address_elem_list),
                    ))
            
            phone_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('phone', )}},
                    {'tag': '{http://www.w3.org/1999/xhtml}img'},
                    ))
            fetch_data['phone'] = ' | '.join(
                    elem.get('src', '') for elem in phone_elem_list
                    )
            
            email_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('email', )}},
                    ))
            fetch_data['email'] = ' | '.join(
                    ' '.join(frag.strip() for frag in elem.itertext()) for elem in email_elem_list
                    )
            
            www_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('www', )}},
                    ))
            fetch_data['www'] = ' | '.join(
                    ' '.join(frag.strip() for frag in elem.itertext()) for elem in www_elem_list
                    )
            
            work_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('work', )}},
                    ))
            fetch_data['work'] = ' | '.join(
                    ' '.join(filter(
                            lambda text: text and text != 'Деятельность:',
                            (frag.strip() for frag in elem.itertext()),
                            )) for elem in work_elem_list
                    )
            
            rubriks_elem_list = et_find.find((doc,), (
                    {'tag': '{http://www.w3.org/1999/xhtml}html'},
                    {'tag': '{http://www.w3.org/1999/xhtml}body'},
                    {'in_attrib': {'class': ('firminfo', )}},
                    {'in_attrib': {'class': ('rubriks', )}},
                    ))
            fetch_data['rubriks'] = ' | '.join(
                    ' '.join(filter(
                            lambda text: text and text != 'Рубрики:',
                            (frag.strip() for frag in elem.itertext()),
                            )) for elem in rubriks_elem_list
                    )
            
            if on_fetch is not None:
                on_fetch(fetch_url, fetch_data)
        except Exception as e:
            if on_error is not None:
                on_error(fetch_url, type(e), str(e))

class BulkDataCtx:
    pass

def init_bulk_data_ctx(bulk_data_ctx):
    bulk_data_ctx.lock = threading.RLock()
    bulk_data_ctx.new_url_list = [
            url.urljoin(SITE_URL, 'firms/'),
            ]
    bulk_data_ctx.done_url_list = []

def bulk_data_fetch(
        bulk_data_ctx,
        thread_count,
        on_scheduled=None,
        on_begin=None,
        on_fetch=None,
        on_error=None,
        on_done=None,
        ):
    with bulk_data_ctx.lock:
        bulk_data_ctx.url_list = bulk_data_ctx.new_url_list
        bulk_data_ctx.new_url_list = []
        
        if not bulk_data_ctx.url_list:
            raise StopIteration
    
    thread_list = tuple(
            threading.Thread(target=lambda: data_fetch_thread(
                    bulk_data_ctx,
                    on_scheduled=on_scheduled,
                    on_begin=on_begin,
                    on_fetch=on_fetch,
                    on_error=on_error,
                    ), daemon=True)
            for thread_i in range(thread_count)
            )
    
    for thread in thread_list:
        thread.start()
    
    def wait_thread():
        for thread in thread_list:
            thread.join()
        
        if on_done is not None:
            on_done()
    
    threading.Thread(target=wait_thread, daemon=True).start()
