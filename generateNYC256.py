from __future__ import print_function

import csv
import datetime
import mmap
import multiprocessing
import threading
from Queue import Queue
from threading import Thread

import mercantile
import requests
from pyproj import *

""" Generates a csv file, so that each row contains a random bounding box, and width and height values equal to 256. These
parameters are used in the WMS GetMap request. The bounding boxes refer to areas in NYC.

Usage: python generateNYC256.py <csv file to be generated> <number of rows of csv file>

"""


def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)


def salehi():
    with open(sys.argv[1], 'wb') as csvFile:
        fileWriter = csv.writer(csvFile, delimiter=',')
        numberOfRows = int(sys.argv[2])
        sourceProj = Proj(init='epsg:4326')
        targetProj = Proj(init='epsg:3857')
        # x1, y1 = transform(sourceProj, targetProj, 43.0, 24.0)
        # x2, y2 = transform(sourceProj, targetProj, 63.0, 40.0)

        for i in mercantile.tiles(43.85, 25.25, 63.39, 39.87, [15], True):
            bbox = mercantile.bounds(i)
            assert isinstance(bbox, mercantile.LngLatBbox)
            min_x, min_y = bbox.west, bbox.south
            max_x, max_y = bbox.east, bbox.north
            row = (min_x, min_y, max_x, max_y, 256, 256)
            fileWriter.writerow(row)
            numberOfRows = numberOfRows - 1
            if numberOfRows <= 0:
                break


def generate_all():
    with open(sys.argv[1], 'a') as csvFile:
        fileWriter = csv.writer(csvFile, delimiter=',')
        numberOfRows = int(sys.argv[2])
        iran_bbox = [4921321.6291, 2900938.0975, 7024868.6475, 4828374.2027]
        iran_rightbottom_point = [6859229.6072, 2897727.7423]
        base_tile = [4902976.742324346, 4775785.527257811, 4904199.7347769085, 4777008.519710374]  # 15
        base_tile = [4903588.238550628, 4776397.023484095, 4904199.7347769085, 4777008.519710374]  # 16
        base_tile = [4903893.986663768, 4776397.023484095, 4904199.7347769085, 4776702.7715972345]  # 17
        dx = base_tile[2] - base_tile[0]
        dy = base_tile[3] - base_tile[1]
        m = 1
        tile = base_tile[:]
        while tile[2] <= iran_rightbottom_point[0]:
            tile[:] = [
                tile[0] + dx * m,
                base_tile[1],
                tile[2] + dx * m,
                base_tile[3]]
            while tile[3] >= iran_rightbottom_point[1]:
                fileWriter.writerow(tile)
                numberOfRows -= 1
                if numberOfRows <= 0:
                    exit(0)
                tile[:] = [
                    tile[0],
                    tile[1] - dy * m,
                    tile[2],
                    tile[3] - dy * m]


base = 'http://geoserver/geoserver/gwc/service/wms?service=WMS&request=GetMap&layers=Shiveh:ShivehGSLD256&styles=&format=image/png&transparent=false&version=1.1.1&height=256&width=256&srs=EPSG:3857&bbox='
ans = {}


class ReqThread(threading.Thread):
    def __init__(self, n, bbox, timeout):
        threading.Thread.__init__()
        self.n = n
        self.bbox = bbox
        self.timeout = timeout
        self.req = None

    def run(self):
        self.req = requests.get('%s%s' % (base, self.bbox), timeout=self.timeout)
        if self.req.status_code == 200:
            ans[self.n] = 'y'
        else:
            ans[self.n] = 'n'


class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        try:
            self.start()
        except:
            pass

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()


def bulk_req():
    cpus = multiprocessing.cpu_count()
    pool = ThreadPool(10000)
    print('Number of threads: {}'.format(len(threading.enumerate())))
    numOfLines = int(os.popen('wc -l %s' % sys.argv[1]).read().split(' ')[0])
    with open(sys.argv[1], 'r') as csvFile:
        mm = mmap.mmap(csvFile.fileno(), 0, prot=mmap.PROT_READ)
        start = datetime.datetime.now()
        for n, bbox in enumerate(iter(mm.readline, "")):
            pool.add_task(requests.get, '%s%s' % (base, bbox))
            if n % 1000 == 0:
                print(n)
                elapsed = datetime.datetime.now() - start
                eta = (elapsed.seconds * numOfLines) / (n * 60)
                print("%d seconds" % elapsed.seconds)
                print("%d ETA(minutes)" % eta)
        mm.close()


if __name__ == '__main__':
    start = datetime.datetime.now()
    generate_all()
    elapsed = datetime.datetime.now() - start
    print("Total: %d" % elapsed.seconds)
