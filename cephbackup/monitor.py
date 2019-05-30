#!/usr/bin/python 
#coding:utf-8
import urllib,urllib2,json
import commands
import time
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )


'''
#    monitor = WeChat('https://qyapi.weixin.qq.com/cgi-bin')
#    monitor.sendMessage('gx',result)
'''


class WeChat(object):
        __token_id = ''
        # init attribute
        def __init__(self,url,corpid,secret):
                self.__url = url.rstrip('/')
                self.__corpid = 'wxe7153de2e49ad499'
                #self.__secret = 'R-0OAP12Vokwg8e1nlRqWu04hZkTcHypcZV71fnRzgU'
		self.__secret = '7fkLj-lHJDSEwSYxHsmdZzLAHAE-IvqnFyDIHdjY-Ks'		

        # Get TokenID
        def __authID(self):
                params = {'corpid':self.__corpid, 'corpsecret':self.__secret}
                data = urllib.urlencode(params)

                content = self.__getToken(data)

                try:
                        self.__token_id = content['access_token']
                        #print content['access_token']
                except KeyError:
                        raise KeyError

        # Establish a connection
        def __getToken(self,data,url_prefix='/'):
                url = self.__url + url_prefix + 'gettoken?'
                try:
                        response = urllib2.Request(url + data)
                except KeyError:
                        raise KeyError
                result = urllib2.urlopen(response)
                content = json.loads(result.read())
                return content

        # Get sendmessage url
        def __postData(self,data,url_prefix='/'):
                url = self.__url + url_prefix + 'message/send?access_token=%s' % self.__token_id
                request = urllib2.Request(url,data)
                try:
                        result = urllib2.urlopen(request)
                except urllib2.HTTPError as e:
                        if hasattr(e,'reason'):
                                print 'reason',e.reason
                        elif hasattr(e,'code'):
                                print 'code',e.code
                        return 0
                else:
                        content = json.loads(result.read())
                        result.close()
                return content

        # send message
        def sendMessage(self,touser,message):
                self.__authID()

                data = json.dumps({
                        'touser':touser,
                        'toparty':"2",
                        'msgtype':"text",
                        'agentid':"1",
                        'text':{
                                'content':message
                        },
                        'safe':"0"
                },ensure_ascii=False)

                response = self.__postData(data)
                print response

