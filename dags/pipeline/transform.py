import xml.etree.ElementTree as ET


def iter_series_obs_rows(xml_bytes: bytes):
    """
    Generator for SDMX-ish XML:
      row = {**Series.attrib, **Obs.attrib}
    """
    root = ET.fromstring(xml_bytes)
    for series in root.findall(".//{*}Series"):
        s_attrs = series.attrib
        for obs in series.findall(".//{*}Obs"):
            yield {**s_attrs, **obs.attrib}
