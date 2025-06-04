import os
import pandas as pd

class ExceptionsManager:
    """Менеджер для работы с исключениями адресов."""
    
    def __init__(self, exceptions_file="Exceptions.xlsx"):
        self.exceptions_file = exceptions_file
        self.exceptions = {}  # {неправильный_адрес: (правильный_адрес, ключ)}
        self._load_exceptions()
        
    def _normalize_address(self, address: str) -> str:
        """Нормализует адрес для сравнения."""
        if not address:
            return ""
        print(f"Нормализация адреса: {address}")  # Отладочная информация
        # Приводим к верхнему регистру
        address = address.upper()
        print(f"После приведения к верхнему регистру: {address}")  # Отладочная информация
        # Заменяем различные варианты написания на стандартные
        replacements = {
            'УЛИЦА': 'УЛ.',
            'УЛ ': 'УЛ.',
            'УЛ.': 'УЛ.',
            'ПРОСПЕКТ': 'ПР-КТ',
            'ПРОСП': 'ПР-КТ',
            'ПР-Т': 'ПР-КТ',
            'ПРОЕЗД': 'ПР-Д',
            'ПР.': 'ПР-Д',
            'БУЛЬВАР': 'Б-Р',
            'БУЛ': 'Б-Р',
            'ПЛОЩАДЬ': 'ПЛ.',
            'ПЛ ': 'ПЛ.',
        }
        for old, new in replacements.items():
            if old in address:
                print(f"Замена '{old}' на '{new}'")  # Отладочная информация
                address = address.replace(old, new)
        # Убираем лишние пробелы
        address = ' '.join(address.split())
        print(f"Итоговый нормализованный адрес: {address}")  # Отладочная информация
        return address
        
    def _load_exceptions(self):
        """Загружает исключения из файла."""
        try:
            if os.path.exists(self.exceptions_file):
                print(f"Загрузка исключений из файла: {self.exceptions_file}")
                df = pd.read_excel(self.exceptions_file)
                print(f"Содержимое файла исключений:\n{df}")
                
                # Загружаем данные в словарь с нормализацией адресов
                self.exceptions = {}
                for _, row in df.iterrows():
                    address = self._normalize_address(row['address'])
                    correct_address = row['correct_address']
                    key = row['key']
                    self.exceptions[address] = (correct_address, key)
                
                print(f"Загруженные исключения: {self.exceptions}")
            else:
                print(f"Файл исключений не найден: {self.exceptions_file}")
        except Exception as e:
            print(f"Ошибка при загрузке файла исключений: {str(e)}")
            
    def get_key(self, address: str) -> tuple[int | None, str]:
        """Получает ключ для адреса из исключений."""
        print(f"\nПоиск ключа для адреса: {address}")  # Отладочная информация
        normalized_address = self._normalize_address(address)
        print(f"Нормализованный адрес для поиска: {normalized_address}")  # Отладочная информация
        print(f"Доступные исключения: {self.exceptions}")  # Отладочная информация
        
        if normalized_address in self.exceptions:
            correct_address, key = self.exceptions[normalized_address]
            print(f"Адрес найден в исключениях: correct_address={correct_address}, key={key}")  # Отладочная информация
            if key is None:
                return None, "адрес не существует"
            return key, ""
        print(f"Адрес не найден в исключениях")  # Отладочная информация
        return None, "адрес не найден"
        
    def get_correct_address(self, address: str) -> str | None:
        """Получает правильный адрес для адреса из исключений."""
        normalized_address = self._normalize_address(address)
        if normalized_address in self.exceptions:
            correct_address, _ = self.exceptions[normalized_address]
            return correct_address
        return None 