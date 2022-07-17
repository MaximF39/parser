import json
import os
import shutil
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import asyncio
import requests

from urls import urls
from config import *


def get_max_product_cat(type_):
    """
    igrotoys.py parser_type max_prod max_cat
    igrotoys.py update/create int int
    exmple:
        igrotoys.py update 100 200
    """
    argv = sys.argv

    if len(argv) == 4:
        return int(argv[2]), int(argv[3])
    else:
        type_ = "_" + str(type_) if not type_.isParser else ""
        if (prod := f"{prefix_product}{type_}") in globals() \
                and (cat := f"{prefix_cat}{type_}") in globals():
            return globals()[prod], globals()[cat]
        else:
            if degub:
                return get_max_product_cat(eval(debug_default_type))
            else:
                raise AttributeError("Неизвестный аргумент, используйте:",
                                     ", ".join(filter(lambda x: not x.startswith("__"), vars(TP))))


def get_type():
    if degub and (len(sys.argv) == 1 or sys.argv[1].isdigit()):
        value = eval(debug_default_type)
        print("Не было передано значение, используется дефолтное значение", value)
        return value
    try:
        return getattr(TP, sys.argv[1])
    except AttributeError:
        raise AttributeError("Неизвестный аргумент, используйте:",
                             ", ".join(filter(lambda x: not x.startswith("__"), vars(TP))))


class TypeParser:
    @dataclass
    class Value:
        name: str

        def __new__(cls, name, *args, **kwargs):
            class_ = super().__new__(cls)
            setattr(cls, f"is{name.title()}", False)
            setattr(class_, f"is{name.title()}", True)
            return class_

        def __str__(self):
            return self.name

    create = Value("create")
    update = Value("update")
    parser = Value("parser")

    def __new__(cls, *args, **kwargs):
        raise "Нельзя создать класс"


TP = TypeParser


class SiteUrl:
    parser_create = urls["parser_create"]  # your url
    parser_update = urls["parser_update"]
    parser = urls["parser"]

    def __new__(cls, type_):
        match type_:
            case TP.update:
                return cls.parser_update
            case TP.create:
                return cls.parser_create
            case TP.parser:
                return cls.parser


SU = SiteUrl


class I_Parser(ABC):

    @property
    def mutex(self):
        return self._mutex

    @property
    def products(self):
        return self._prods

    @property
    def categories(self):
        return self._cats

    @property
    @abstractmethod
    def isParseCat(self):
        ...

    def append_category(self, category):
        self.append(category, "cat")

    def append_start(self, value=""):
        self.append(value, "start")

    def append_end(self, value=""):
        self.append(value, "end")

    def append_product(self, product):
        self.append(product, "product")

    def append(self, values, action: str):
        self._append(values, action)

    @abstractmethod
    def _append(self, values, action: str):
        ...


@dataclass
class Parser(I_Parser):
    _parser_name: str
    _is_save_json: bool = field(default=True, kw_only=True)

    _type: TP = field(init=False)
    _url: str = field(init=False)
    _prod_max: int = field(init=False)
    _cat_max: int = field(init=False)

    _prods: list = field(default_factory=list, init=False)
    _cats: list = field(default_factory=list, init=False)
    _mutex: threading.Lock = field(default_factory=threading.Lock, init=False)
    start_time: float = field(default_factory=time.time, init=False)

    def __post_init__(self):
        self._folder_name = self._get_folder()
        self._type = get_type()
        self._prod_max, self._cat_max = get_max_product_cat(self._type)
        if not self._cat_max and self.isParseCat:
            raise KeyError("Not found _cat_max")
        self._url = SU(self._type)
        if self._is_save_json:
            self._update_folder()

    def _get_folder(self):
        folder_name = prefix_folder + self._parser_name
        return folder_name

    def _update_folder(self):
        if os.path.exists(self._folder_name):
            shutil.rmtree(self._folder_name)
            print("Папка удалена", self._folder_name)
        os.mkdir(self._folder_name)
        print("Папка создана", self._folder_name)

    def _remove_value(self, var, count):
        self._mutex.acquire()
        del var[:count]
        self._mutex.release()

    @property
    def isParseCat(self):
        return self._type.isCreate or self._type.isParser

    def _append(self, values, action: str):
        """
        :values dict product or category    \n
        :action in ("end", "start", "cat", "product") """
        if action == "end" or action == "start":
            if self._prods:
                self._send_request(self._prods, "product")
                self._prods.clear()
            if self._type.isUpdate:
                self._send_request(values, action)
        else:
            if action == "cat" and self.isParseCat:
                self._cats.append(values)
                if len(self._cats) >= self._cat_max:
                    self._send_request(self._cats[:self._cat_max], action)
                    self._remove_value(self._cats, self._cat_max)
            elif action == "product":
                self._prods.append(values)
                if self._cats:
                    self._send_request(self._cats, "cat")
                    self._cats.clear()
                if len(self._prods) >= self._prod_max:
                    self._send_request(self._prods[:self._prod_max], action)
                    self._remove_value(self._prods, self._prod_max)

    def _save_json(self, value, status_code: int):
        if not self._is_save_json:
            return
        value["status_code"] = status_code
        directory = os.path.join(os.getcwd(), self._folder_name)
        path_ = os.path.join(directory, f"{prefix_name}{self._parser_name}.json")
        if not os.path.exists(path_):
            with open(path_, "w", encoding="utf-8") as f:
                json.dump([value], f, ensure_ascii=False, indent=4)
        else:
            with open(path_, "rb+") as f:
                f.seek(-1, os.SEEK_END)
                f.truncate()
            with open(path_, "a", encoding="utf-8") as f:
                f.write(",")
                f.write(json.dumps(value, ensure_ascii=False, indent=4))
                f.write("]")

    async def _async_send_request(self, values, action):
        myobj = {"action": action, "site": self._parser_name, "values": values}
        if degub:
            self._save_json(myobj, debug_status_code)
            return
        headers = {"Content-type": "application/json", "Accept": "text/plain"}
        x = requests.post(self._url, data=json.dumps(myobj), headers=headers, timeout=300)
        print(x.status_code)
        if x.status_code != 200:
            x = requests.post(self._url, data=json.dumps(myobj))
            print(x.text)
        await asyncio.sleep(0.1)
        self._save_json(myobj, x.status_code)

    def _send_request(self, values, action):
        asyncio.run(self._async_send_request(values, action))

    def __del__(self):
        print(f"{self._parser_name} parsing time: {time.time() - self.start_time:.2f} second")
