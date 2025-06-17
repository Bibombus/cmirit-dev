import argparse


def make_args_parser() -> argparse.ArgumentParser:
    """Создает парсер параметров запуска программы из командной строки.

    Returns:
        argparse.ArgumentParser: Готовый ко использованию парсер параметров.
    """

    p = argparse.ArgumentParser(description='Программа для разбора и определения адреса (по г. Череповец).')

    # Общие параметры
    common_params = [
        {
            'short': '-g',
            'full': '--gui',
            'action': 'store_true',
            'default': False,
            'required': False,
            'help': 'Запустить программу в режиме графического интерфейса. По умолчанию - консольный режим.'
        },
        {
            'short': '-v',
            'full': '--verbose',
            'action': 'store_true',
            'default': False,
            'required': False,
            'help': 'Необходимо ли выводить информацию о ходе работы в консоль. По умолчанию - нет.'
        },
        {
            'short': '-dbf',
            'full': '--db_export_file',
            'default': "./DB_EXPORT.xlsx",
            'required': False,
            'help': 'Путь к excel-файлу выгрузки из БД. По умолчанию - файл "DB_EXPORT.xlsx", находящийся в папке с программой.'
        },
        {
            'short': '-dbs',
            'full': '--db_export_sheet_name',
            'default': "Sheet 1",
            'required': False,
            'help': 'Название листа в файле выгрузки из БД. По умолчанию - "Sheet 1".'
        }
    ]

    # Параметры для работы с excel-файлами
    excel_file_params = [
        {
            'short': '-i',
            'full': '--input_file',
            'default': "./input.xlsx",
            'required': False,
            'help': 'Путь к excel-файлу с входными данными. По умолчанию - файл "input.xlsx", находящийся в папке с программой. Возможно указывать путь, относительно данной папки.'
        },
        {
            'short': '-isn',
            'full': '--input_sheet_name',
            'default': "Sheet 1",
            'required': False,
            'help': 'Название листа в excel-файле с входными данными. По умолчанию - "Sheet 1".'
        },
        {
            'short': '-icn',
            'full': '--input_column_name',
            'default': "Address",
            'required': False,
            'help': 'Название столбца с сырыми адресами в excel-файле с входными данными. По умолчанию - "Address".'
        },
        {
            'short': '-o',
            'full': '--output_file',
            'default': "./output.xlsx",
            'required': False,
            'help': 'Путь к excel-файлу с выходными данными. По умолчанию - файл "output.xlsx", находящийся в папке с программой. Возможно указывать путь, относительно данной папки.'
        }
    ]


    # dialect+driver://username:password@host:port/database (https://docs.sqlalchemy.org/en/20/core/engines.html)
    db_params = [
        {
            'short': '-dbms',
            'full': '--database_managment_system',
            'default': None,
            'required': False,
            'choices': ['postgres', 'oracle', 'ms-sql-server'],
            'help': 'Желаемая СУБД.'
        },
        {
            'short': '-host',
            'full': '--host',
            'default': None,
            'required': False,
            'help': 'Хост, на котором запущена СУБД. Указывается при работе программы не с excel-файлом, а с СУБД.'
        },
        {
            'short': '-p',
            'full': '--port',
            'default': None,
            'required': False,
            'help': 'Порт для подключения к СУБД.'
        },
        {
            'short': '-db',
            'full': '--database',
            'default': None,
            'required': False,
            'help': 'Название базы данных.'
        },
        {
            'short': '-u',
            'full': '--user',
            'default': None,
            'required': False,
            'help': 'Имя пользователя.'
        },
        {
            'short': '-pwd',
            'full': '--password',
            'default': None,
            'required': False,
            'help': 'Пароль.'
        }
    ]

    params = list(common_params)
    params.extend(excel_file_params)
    params.extend(db_params)

    for param in params:
        short, full, = param.pop('short'), param.pop('full')
        p.add_argument(short, full, **param)

    return p