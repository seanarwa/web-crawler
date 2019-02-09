import logging
from datamodel.search.Seantc_datamodel import SeantcLink, OneSeantcUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(SeantcLink)
@GetterSetter(OneSeantcUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Seantc"

    def __init__(self, frame):
        self.app_id = "Seantc"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneSeantcUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = SeantcLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneSeantcUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(SeantcLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")

class Analytics:
    max_out_links = 0
    subdomain_dict = {}

    def save(filename):
        f = open("analytics.txt", "w")
        f.write("max_out_links: " + str(self.max_out_links) + "\n")
        f.write("---- SUBDOMAINS ----\n")
        for key in self.subdomain_dict.keys():
            f.write(key + "\t" + str(self.subdomain_dict[key]) + "\n")
        f.write("--------------------\n")
        f.close()

analytics = Analytics()

def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''

    try:
        root = html.fromstring(rawDataObj.content.encode("utf-8"));
        root.make_links_absolute(rawDataObj.url.encode("utf-8"))
        outputLinks.extend(root.xpath('//a/@href'))
    except:
        pass

    if rawDataObj.http_code != 200:
        print("HTTP CODE: " + str(rawDataObj.http_code))
        f = open("analytics-http-code.txt", "a")
        f.write("URL:" + rawDataObj.url + " HTTP_CODE:" + str(rawDataObj.http_code) + "\n")
        f.close()

    # Analytics
    if len(outputLinks) > analytics.max_out_links:
        analytics.max_out_links = len(outputLinks)

    # Analytics
    parsed = urlparse(rawDataObj.url)
    try:
        if ".ics.uci.edu" in parsed.hostname:
            subdomain = parsed.hostname.replace("www.", "")
            # subdomain = parsed.hostname.split('.')[0].replace("www", "") + ".ics.uci.edu"
            if subdomain != "":
                if subdomain not in analytics.subdomain_dict:
                    analytics.subdomain_dict[subdomain] = 1
                else:
                    analytics.subdomain_dict[subdomain] += 1
    except:
        pass

    analytics.save("analytics.txt")

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)

    paths = url.split("/")
    for i in range(len(paths) - 2):
        if(paths[i] == paths[i + 1] and paths[i + 1] == paths[i + 2]):
            return False;

    if parsed.scheme not in set(["http", "https"]):
        return False

    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False
