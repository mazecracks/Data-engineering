# pipeline/transform.py
import xml.etree.ElementTree as ET
from typing import Iterator, Dict, Any, IO


def iter_series_obs_rows(xml_file: IO[bytes]) -> Iterator[Dict[str, Any]]:
    current_series_attrs: Dict[str, Any] | None = None

    context = ET.iterparse(xml_file, events=("start", "end"))

    for event, elem in context:
        tag = elem.tag.split("}")[-1]

        if event == "start" and tag == "Series":
            current_series_attrs = dict(elem.attrib)

        elif event == "end" and tag == "Obs":
            if current_series_attrs:
                yield {**current_series_attrs, **dict(elem.attrib)}
            elem.clear()

        elif event == "end" and tag == "Series":
            elem.clear()
            current_series_attrs = None
