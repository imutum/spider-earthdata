from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version(__package__)
except PackageNotFoundError:
    __version__ = "unknown version"


from spided.downloader import Downloader
from spided.platform import EarthDataDownloader, EarthExplorerDownloader
from spided.strategy import StrategyCSV