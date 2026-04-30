from __future__ import annotations

import re
from dataclasses import dataclass
from html import unescape
from typing import Iterable, cast

from lxml import etree, html

_HIDDEN_TEXT_TAGS = {"script", "style", "noscript", "svg", "path", "use"}


@dataclass(slots=True, frozen=True)
class _AttributeCondition:
    name: str
    operator: str
    value: str


@dataclass(slots=True, frozen=True)
class _SimpleSelector:
    tag: str | None
    classes: tuple[str, ...]
    attributes: tuple[_AttributeCondition, ...]


class RegexMatch:
    def __init__(self, value: str) -> None:
        self._value = value

    def text_content(self) -> str:
        return self._value

    def get_all_text(self) -> str:
        return self._value

    @property
    def text(self) -> str:
        return self._value

    def __str__(self) -> str:
        return self._value


class HtmlNode:
    def __init__(
        self, element: etree._Element, *, url: str = "", status_code: int | None = None
    ) -> None:
        self._element = element
        self.url = url
        self.status_code = status_code
        self.status = status_code

    def css(self, selector: str) -> list["HtmlNode"]:
        return [
            HtmlNode(node, url=self.url, status_code=self.status_code)
            for node in _select_nodes(self._element, selector)
        ]

    def css_first(
        self,
        selector: str,
        *,
        identifier: str | None = None,
        auto_save: bool = False,
        auto_match: bool = False,
    ) -> "HtmlNode | None":
        del identifier, auto_save, auto_match
        matches = self.css(selector)
        return matches[0] if matches else None

    def find_similar(self) -> list["HtmlNode"]:
        parent = self._element.getparent()
        if parent is None:
            return []

        tag = self._element.tag
        classes = tuple(sorted(self._element.get("class", "").split()))
        matches: list[HtmlNode] = []
        for sibling in parent:
            if not isinstance(sibling.tag, str) or sibling is self._element or sibling.tag != tag:
                continue
            sibling_classes = tuple(sorted(sibling.get("class", "").split()))
            if sibling_classes == classes:
                matches.append(HtmlNode(sibling, url=self.url, status_code=self.status_code))
        return matches

    def find_by_regex(
        self, pattern: str, *, first_match: bool = False
    ) -> RegexMatch | list[RegexMatch] | None:
        matches = [
            RegexMatch(match.group(0))
            for match in re.finditer(pattern, self.get_all_text(), re.I | re.M)
        ]
        if first_match:
            return matches[0] if matches else None
        return matches

    def save(self, element: object, identifier: str | None = None) -> object:
        del identifier
        return element

    def text_content(self) -> str:
        return _clean_space(" ".join(_iter_visible_text(self._element)))

    def get_all_text(self) -> str:
        return self.text_content()

    @property
    def text(self) -> str:
        return self.text_content()

    @property
    def attrib(self) -> dict[str, str]:
        return dict(self._element.attrib)

    def get(self, attr_name: str, default: str | None = None) -> str | None:
        return cast(str | None, self._element.get(attr_name, default))

    def __str__(self) -> str:
        return cast(str, html.tostring(self._element, encoding="unicode"))


class HtmlDocument(HtmlNode):
    def __init__(self, markup: str, *, url: str, status_code: int | None = None) -> None:
        parser = html.HTMLParser(encoding="utf-8")
        root = html.fromstring(markup or "<html></html>", parser=parser, base_url=url)
        super().__init__(root, url=url, status_code=status_code)
        self.markup = markup


def parse_html_document(markup: str, *, url: str, status_code: int | None = None) -> HtmlDocument:
    return HtmlDocument(markup, url=url, status_code=status_code)


def _select_nodes(root: etree._Element, selector: str) -> list[etree._Element]:
    selectors = _split_selector_groups(selector)
    matches: list[etree._Element] = []
    seen: set[int] = set()

    for selector_group in selectors:
        chain = [_parse_simple_selector(chunk) for chunk in _split_descendant_chain(selector_group)]
        current: list[etree._Element] = [root]
        for step_index, step in enumerate(chain):
            next_nodes: list[etree._Element] = []
            step_seen: set[int] = set()
            for base in current:
                candidates = _iter_candidates(base, include_self=step_index == 0)
                for candidate in candidates:
                    if _matches_selector(candidate, step) and id(candidate) not in step_seen:
                        step_seen.add(id(candidate))
                        next_nodes.append(candidate)
            current = next_nodes
            if not current:
                break

        for node in current:
            node_id = id(node)
            if node_id not in seen:
                seen.add(node_id)
                matches.append(node)

    return matches


def _iter_candidates(root: etree._Element, *, include_self: bool) -> Iterable[etree._Element]:
    if include_self and isinstance(root.tag, str):
        yield root
    for element in root.iterdescendants():
        if isinstance(element.tag, str):
            yield element


def _split_selector_groups(selector: str) -> list[str]:
    parts: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    bracket_depth = 0

    for char in selector:
        if quote:
            buffer.append(char)
            if char == quote:
                quote = None
            continue

        if char in {"'", '"'}:
            quote = char
            buffer.append(char)
            continue

        if char == "[":
            bracket_depth += 1
            buffer.append(char)
            continue

        if char == "]":
            bracket_depth = max(0, bracket_depth - 1)
            buffer.append(char)
            continue

        if char == "," and bracket_depth == 0:
            part = "".join(buffer).strip()
            if part:
                parts.append(part)
            buffer = []
            continue

        buffer.append(char)

    tail = "".join(buffer).strip()
    if tail:
        parts.append(tail)
    return parts


def _split_descendant_chain(selector: str) -> list[str]:
    parts: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    bracket_depth = 0

    for char in selector:
        if quote:
            buffer.append(char)
            if char == quote:
                quote = None
            continue

        if char in {"'", '"'}:
            quote = char
            buffer.append(char)
            continue

        if char == "[":
            bracket_depth += 1
            buffer.append(char)
            continue

        if char == "]":
            bracket_depth = max(0, bracket_depth - 1)
            buffer.append(char)
            continue

        if char.isspace() and bracket_depth == 0:
            part = "".join(buffer).strip()
            if part:
                parts.append(part)
                buffer = []
            continue

        buffer.append(char)

    tail = "".join(buffer).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_simple_selector(selector: str) -> _SimpleSelector:
    remainder = selector.strip()
    tag_match = re.match(r"^[A-Za-z][A-Za-z0-9_-]*|\*", remainder)
    tag: str | None = None
    if tag_match:
        tag = tag_match.group(0)
        remainder = remainder[tag_match.end() :]
        if tag == "*":
            tag = None

    classes: list[str] = []
    attributes: list[_AttributeCondition] = []

    while remainder:
        if remainder.startswith("."):
            class_match = re.match(r"\.([A-Za-z0-9_-]+)", remainder)
            if class_match is None:
                break
            classes.append(class_match.group(1))
            remainder = remainder[class_match.end() :]
            continue

        if remainder.startswith("["):
            attr_match = re.match(
                r"\[(?P<name>[A-Za-z0-9_:-]+)(?:(?P<op>\*=|=)(?P<value>\"[^\"]*\"|'[^']*'|[^\]]+))?\]",
                remainder,
            )
            if attr_match is None:
                break
            raw_value = attr_match.group("value") or ""
            clean_value = raw_value.strip().strip("'\"")
            attributes.append(
                _AttributeCondition(
                    name=attr_match.group("name"),
                    operator=attr_match.group("op") or "exists",
                    value=unescape(clean_value),
                )
            )
            remainder = remainder[attr_match.end() :]
            continue

        break

    return _SimpleSelector(tag=tag, classes=tuple(classes), attributes=tuple(attributes))


def _matches_selector(element: etree._Element, selector: _SimpleSelector) -> bool:
    if selector.tag and element.tag.lower() != selector.tag.lower():
        return False

    class_tokens = set((element.get("class") or "").split())
    for class_name in selector.classes:
        if class_name not in class_tokens:
            return False

    for condition in selector.attributes:
        attr_value = element.get(condition.name)
        if condition.operator == "exists":
            if attr_value is None:
                return False
            continue
        if attr_value is None:
            return False
        if condition.operator == "=" and attr_value != condition.value:
            return False
        if condition.operator == "*=" and condition.value not in attr_value:
            return False

    return True


def _clean_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _iter_visible_text(element: etree._Element) -> Iterable[str]:
    if not isinstance(element.tag, str):
        return

    tag_name = element.tag.lower()
    if tag_name in _HIDDEN_TEXT_TAGS:
        if element.tail:
            yield element.tail
        return

    if element.text:
        yield element.text

    for child in element:
        yield from _iter_visible_text(child)

    if element.tail:
        yield element.tail


__all__ = ["HtmlDocument", "HtmlNode", "RegexMatch", "parse_html_document"]
