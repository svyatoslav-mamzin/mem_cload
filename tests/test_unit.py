from optparse import OptionParser
import memcache_connect


def cases(cases_list):
    def deco(func):
        def wrapper(*ar):
            for case in cases_list:
                func(case, *ar)
        return wrapper
    return deco


fake_cache = {}


def mock_memc_get(addr, *args, **kwargs):
    addr = fake_cache.get(addr)
    return addr.get(args[1]) if addr is not None else None


def mock_memc_set(addr, *args, **kwargs):
    fake_cache.update({addr: {args[0]: args[1]}})


memcache_connect.memc_get = mock_memc_get
memcache_connect.memc_set = mock_memc_set
from memc_load import process_batch, parse_appsinstalled, AppsInstalled, insert_appsinstalled

op = OptionParser()
op.add_option("-t", "--test", action="store_true", default=False)
op.add_option("-l", "--log", action="store", default=None)
op.add_option("--dry", action="store_true", default=False)
op.add_option("--pattern", action="store", default="data/*.tsv.gz")
op.add_option("--idfa", action="store", default="127.0.0.1:33013")
op.add_option("--gaid", action="store", default="127.0.0.1:33014")
op.add_option("--adid", action="store", default="127.0.0.1:33015")
op.add_option("--dvid", action="store", default="127.0.0.1:33016")
(options, args) = op.parse_args()

device_memc = {
        "idfa": '127.0.0.1:33013',
        "gaid": '127.0.0.1:33014',
        "adid": '127.0.0.1:33015',
        "dvid": '127.0.0.1:33016',
    }


@cases([
    {'test_line': "idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t-86.9886210214\t28.4049776771\t3914,2301,5343,6292,7196,7849\n",
     'result': AppsInstalled(dev_type='idfa',
                             dev_id='f8abee8b1485bf8b0d0630f3f96c97f8',
                             lat=-86.9886210214,
                             lon=28.4049776771,
                             apps=[3914, 2301, 5343, 6292, 7196, 7849])},
    {'test_line': "idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t3914,2301,5343,6292,7196,7849\n",
         'result': None}
    ])
def test_parse_appsinstalled(data):
    assert parse_appsinstalled(data['test_line']) == data['result']


@cases([{'test_line': ["idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t3914,2301,5343,6292,7196,7849\n"],
         'result': (0, 1,)},
        {'test_line': ["idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t-86.9886210214\t28.4049776771\t3914,2301,7196,7849\n"],
         'result': (1, 0,)},
        {'test_line': ["idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t3914,2301,5343,6292,7196,7849\n",
                        "idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t-86.9886210214\t28.4049776771\t3914,2301,7196,7849\n",
                        "idfaaa\tf8abee8b1485bf8b0d0630f3f96c97f8\t-86.9886210214\t28.4049776771\t3914,2301,7196,7849\n"
                       ],
         'result': (1, 2,)},
        ])
def test_process_batch(data):
    assert process_batch(data['test_line'], device_memc, options) == data['result']


def test_insert_appsinstalled():
    appsinstalled = AppsInstalled(dev_type='idfa',
                  dev_id='f8abee8b1485bf8b0d0630f3f96c97f8',
                  lat=-86.9886210214,
                  lon=28.4049776771,
                  apps=[3914, 2301, 5343, 6292, 7196, 7849])
    assert insert_appsinstalled('127.0.0.1:33013', appsinstalled) == True
    assert fake_cache.get('127.0.0.1:33013') is not None
    assert fake_cache.get('127.0.0.1:33013').get('idfa:f8abee8b1485bf8b0d0630f3f96c97f8') is not None
