from unittest import TestCase

import pandas as pd


from ..Linker import Linker, NotInDBException, UnresolvedAmbigiuty
from ..AddresInfo import Address, Street, StreetType

class TestLinker_load(TestCase):

    def setUp(self):
        self.df = pd.DataFrame()
        self.right_columns = {
            "Type": None,
            "Name": None,
            "House": None,
            "Flat_start": None,
            "Flat_end": None,
            "Key": None,
        }

        self.wrong_columns = {
            "Some": None,
            "Wrong": None,
            "Columns": None,
            "And": None,
            "Some": None,
            "Right": None,
            "Type": None,
            "Name": None,
        }

    def tearDown(self):
        del self.df

    def test_empty_df(self):
        with self.assertRaises(AssertionError):
            self.assertIsInstance(Linker.load(self.df), Linker)

    def test_empty_df_with_wrong_columns(self):
        self.df = self.df.assign(**self.wrong_columns)
        with self.assertRaises(AssertionError):
            self.assertIsInstance(Linker.load(self.df), Linker)

    def test_empty_df_with_right_columns(self):
        self.df = self.df.assign(**self.right_columns)
        self.assertIsInstance(Linker.load(self.df), Linker)


class TestLinker_link(TestCase):

    def setUp(self):
        self.empty_df_with_columns = pd.DataFrame(
            columns=["Type", "Name", "House", "Flat_start", "Flat_end", "Key"]
        )
        self.filled_df = pd.read_excel("./DB_EXPORT.xlsx", "Sheet 1")

        self.empty_linker = Linker.load(self.empty_df_with_columns)
        self.linker = Linker.load(self.filled_df)
        self.last_key = 0

    def tearDown(self):
        del self.empty_df_with_columns
        del self.filled_df
        del self.empty_linker
        del self.linker
        del self.last_key

    def get_key(self, name, type, house) -> int:
        series = self.filled_df.loc[
            (self.filled_df.Name == name)
            & (self.filled_df.Type == type)
            & (self.filled_df.House == house)
        ]
        return series.Key.values[0]

    def test_address_is_none_no_other_params(self):
        self.assertIsNone(self.linker.link(None))

    def test_address_is_empty_no_other_params(self):
        self.assertIsNone(self.linker.link(Address()))

    def test_address_without_house_no_other_params(self):
        addr = Address(street=Street("Победы", StreetType.PROSPECT), flat=10)
        self.assertIsNone(self.linker.link(addr))

    def test_address_without_name_no_other_params(self):
        addr = Address(street=Street(None, StreetType.PROSPECT), house="10б", flat=10)
        self.assertIsNone(self.linker.link(addr))

    def test_filled_address_right_but_not_in_db(self):
        addr = Address.fromStr("ул Металлургов 18 15")
        with self.assertRaises(NotInDBException):
            self.linker.link(addr)

    def test_address_without_flat_not_in_db(self):
        addr = Address.fromStr("ул Первомайская 34б")
        with self.assertRaises(NotInDBException):
            self.linker.link(addr)

    def test_address_without_flat_in_db_no_other_params(self):
        addr = Address.fromStr("пркт советский 57")
        expected = self.get_key("СОВЕТСКИЙ", "ПР-КТ", "57")
        key = self.linker.link(addr)
        self.assertEqual(expected, key)

    def test_address_without_flat_and_type_in_db_no_other_params(self):
        addrs = [
            Address.fromStr("Советский 64а"),
            Address.fromStr("Победы 202"),
        ]
        expected = [
            self.get_key("СОВЕТСКИЙ", "ПР-КТ", "64А"),
            self.get_key("ПОБЕДЫ", "ПР-КТ", "202"),
        ]
        for addr, exp in zip(addrs, expected):
            key = self.linker.link(addr)
            self.assertEqual(exp, key)

    def test_address_without_flat_and_type_in_db_no_require_flat_check(self):
        addrs = [
            Address.fromStr("Советский 64а"),
            Address.fromStr("Победы 202"),
        ]
        expected = [
            self.get_key("СОВЕТСКИЙ", "ПР-КТ", "64А"),
            self.get_key("ПОБЕДЫ", "ПР-КТ", "202"),
        ]
        for addr, exp in zip(addrs, expected):
            key = self.linker.link(addr, require_flat_check=False)
            self.assertEqual(exp, key)

    def test_address_without_flat_in_db_no_require_flat_check_multiplie(self):
        """
        Не путаются ли похожие улицы без чека хаты
        """

        addrs = [
            Address.fromStr("ул Металлургов 2"),
            Address.fromStr("пл Металлургов 5"),
        ]
        expected = [
            self.get_key("МЕТАЛЛУРГОВ", "УЛ.", "2"),
            self.get_key("МЕТАЛЛУРГОВ", "ПЛ.", "5"),
        ]
        for addr, exp in zip(addrs, expected):
            key = self.linker.link(addr, require_flat_check=False)
            self.assertEqual(exp, key)

    def test_address_in_db_def_params_multiplie(self):
        """
        Не путаются ли похожие улицы
        """

        addrs = [
            Address.fromStr("ул Металлургов 2"),
            Address.fromStr("пл Металлургов 5"),
        ]
        expected = [
            self.get_key("МЕТАЛЛУРГОВ", "УЛ.", "2"),
            self.get_key("МЕТАЛЛУРГОВ", "ПЛ.", "5"),
        ]
        for addr, exp in zip(addrs, expected):
            key = self.linker.link(addr)
            self.assertEqual(exp, key)

    def test_address_no_type_in_db_def_params_multiplie_fail(self):
        """
        Невозможно определить тип улицы
        """
        addrs = [
            Address.fromStr("Металлургов 2 10"),
            Address.fromStr("Металлургов 5 20"),
        ]
        for addr in addrs:
            with self.assertRaises(UnresolvedAmbigiuty):
                self.linker.link(addr)

    def test_address_no_type_in_db_def_params_multiplie_pass(self):
        """
        Тут явно можно определить тип улицы, проверяя диапазоны квартир
        """

        addrs = [
            Address.fromStr("Металлургов 2 48"),
            Address.fromStr("Металлургов 5 41"),
        ]
        expected = [
            self.get_key("МЕТАЛЛУРГОВ", "УЛ.", "2"),
            self.get_key("МЕТАЛЛУРГОВ", "ПЛ.", "5"),
        ]
        for exp, addr in zip(expected, addrs):
            key = self.linker.link(addr)
            self.assertEqual(exp, key)

    def test_address_no_type_in_db_no_resolve_multiplie_fail(self):
        """
        А тут не решить без квартир
        """

        addrs = [
            Address.fromStr("Металлургов 2 48"),
            Address.fromStr("Металлургов 5 41"),
        ]
        for addr in addrs:
            with self.assertRaises(UnresolvedAmbigiuty): 
                self.linker.link(addr, False)
