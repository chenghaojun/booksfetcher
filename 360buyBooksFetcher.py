#!/usr/bin/python
# Filename: 360BooksFetcher.py
# Author: Chenghaojun

# http://club.360buy.com/ProductPageService.aspx?method=GetCommentSummaryBySkuId&referenceId=11009824
# http://price.360buy.com/price-b-1A96D87C5B9E060CB3BEEEFECDC9B0809.html

import urllib2
import json
import sqlite3
import time
import re
import sys

class BooksFetcher:
    # about sqlite3 db.
    DB_NAME = "books.sqlite3"
    BOOKS_TABLE_CREATION = "CREATE TABLE IF NOT EXISTS BOOKS(id integer, key text, title text, price float, comment_count int, extras text, gmt_created REAL, gmt_modified REAL)"
    ERRORS_TABLE_CREATION = "CREATE TABLE IF NOT EXISTS ERRORS(id integer, error text, gmt_created REAL, gmt_modified REAL)"
    BOOK_URL = "http://book.360buy.com/%s.html"
    COMMENT_URL = "http://club.360buy.com/ProductPageService.aspx?method=GetCommentSummaryBySkuId&referenceId=%s"

    regexps = {
        'keywords': re.compile("<meta\s+name=\"[kK]eywords\"\s+content=.+\n*.+/>", re.I), 
        'title': re.compile("<title>\n*.+\n*.+\n*</title>", re.I), 
        'priceUrl': re.compile("http://price\.360buy\.com/price\-b\-[a-zA-Z0-9]{33}\.html", re.I)
    }
        
#       10001000, 11041999  #maybe this is the max book id.
    def __init__(self, index, limit):
        self.charset = "GBK"
        self.sIndex = index
        self.priceUrl = None

        # table creation
        conn = sqlite3.connect(self.DB_NAME)
        c = conn.cursor()
        c.execute(self.BOOKS_TABLE_CREATION)
        c.execute(self.ERRORS_TABLE_CREATION)
        conn.commit()

        c.execute('SELECT max(id) FROM books')
        maxId = c.fetchone()[0]
        if maxId is not None:
            self.sIndex = maxId
        self.eIndex = self.sIndex+limit+1

        print 'start at:', time.ctime()

        for pid in range(self.sIndex, self.eIndex+1):
            c.execute("select count(*) from books where id = ?", (pid, ))
            count = c.fetchone()[0]
            if count>0:
                continue

            url = self.BOOK_URL % pid
            self.content = self.fetch(url)

            if self.content is None:
                self.logError(conn, c, pid, url+' no content')
                continue

            key = self.getSkuidkey()
            if key is None:
                self.logError(conn, c, pid, url+' no key')
                continue

            title = self.getTitle()
            price = self.getPrice()
            commentCount = self.getCommentCount(pid)
            keywords = self.getKeywords()

            now = time.time()
            c.execute("insert into books values(?, ?, ?, ?, ?, ?, ?, ?)", (pid, key, unicode(title, "utf-8"), price, commentCount, unicode(keywords, "utf-8"), now, now))
            conn.commit()
            print 'success:%s, price:%f, comment count:%d, title:%s'%(pid, price, commentCount, title)
            time.sleep(3)
        
        c.close()

        print 'finished at:', time.ctime()

    def logError(self, conn, c, pid, url):
        now = time.time()
        c.execute("insert into errors values (?, ?, ?, ?)", (pid, url, now, now))
        conn.commit()
        print 'error:%s, url:%s at:%s'%(pid, url, time.ctime())
    
    def fetch(self, url):
        try:
            content = urllib2.urlopen(url).read()
            type = sys.getfilesystemencoding()
            content = content.decode(self.charset).encode(type)
            return content
        except:
            pass
        return None

    def getSkuidkey(self):
        key = None
        try:
            g = self.regexps['priceUrl'].search(self.content)
            if g:
                key = g.group()
                self.priceUrl = key
                key = key[32:][:-5]
        except:
            pass
        return key;

    def getKeywords(self):
        keywords = None
        g = self.regexps['keywords'].search(self.content)
        if g:
            keywords = g.group()
        return keywords;

    def getTitle(self):
        title = None
        g = self.regexps['title'].search(self.content)
        if g:
            title = g.group()
            title = title[7:][:-8]
        return title;

    def getPrice(self):
        try:
            priceText = urllib2.urlopen(self.priceUrl).read()
            priceText = priceText[12:][:-1] # var jdprice={"P":"\uFFE594.00","I":11009824,"M":"\uFFE594.00"};
            return float(json.loads(priceText)['P'][1:]) # 94.00
        except:
            return -1.0

    def getCommentCount(self, id):
        commentUrl = self.COMMENT_URL %(id)
        try:
            commentText = urllib2.urlopen(commentUrl).read()
            commentText = commentText[3:][:-1]
            commentJson = json.loads(commentText)
            return commentJson['CommentCount']
        except:
            return -1


f = BooksFetcher(10001000, 1000)
