from __future__ import annotations

from xml.etree import ElementTree as ET

from app.voice.prompts import STREAM_NAME


def _to_xml(root: ET.Element) -> str:
    return ET.tostring(root, encoding="unicode")


def build_start_twiml(
    *,
    ws_url: str,
    intro_url: str,
    next_url: str,
    custom_parameters: dict[str, str],
    pause_length: int = 4,
) -> str:
    response = ET.Element("Response")
    start = ET.SubElement(response, "Start")
    stream = ET.SubElement(
        start,
        "Stream",
        {
            "url": ws_url,
            "track": "inbound_track",
            "name": STREAM_NAME,
        },
    )
    for name, value in custom_parameters.items():
        ET.SubElement(stream, "Parameter", {"name": name, "value": value})
    ET.SubElement(response, "Play").text = intro_url
    ET.SubElement(response, "Pause", {"length": str(pause_length)})
    redirect = ET.SubElement(response, "Redirect", {"method": "POST"})
    redirect.text = next_url
    return _to_xml(response)


def build_question_twiml(*, audio_url: str, next_url: str, pause_length: int = 6) -> str:
    response = ET.Element("Response")
    ET.SubElement(response, "Play").text = audio_url
    ET.SubElement(response, "Pause", {"length": str(pause_length)})
    redirect = ET.SubElement(response, "Redirect", {"method": "POST"})
    redirect.text = next_url
    return _to_xml(response)


def build_goodbye_twiml(*, thanks_url: str) -> str:
    response = ET.Element("Response")
    ET.SubElement(response, "Play").text = thanks_url
    ET.SubElement(response, "Hangup")
    return _to_xml(response)
