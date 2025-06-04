import argparse


def make_args_parser() -> argparse.ArgumentParser:
    """Создает парсер параметров запуска программы из командной строки.

    Returns:
        argparse.ArgumentParser: Готовый ко использованию парсер параметров.
    """

    # Общие параметры
    general_params = [
        {
            'short': '-id_name',
            'full': '--identity_column_name',
            'default': None,
            'required': False,
            'help': 'Название ключевого столбца во входных данных. Если не указан, то будет обработан лишь столбец с адресами.'
        },
        {
            'short': '-g',
            'full': '--gui',
            'action': 'store_true',
            'default': False,
            'required': False,
            'help': 'Запуск программы с графической оболочкой.'
        },
        {
            'short': '-db_file',
            'full': '--db_export_file',
            'default': "./DB_EXPORT.xlsx",
            'required': False,
            'help': 'Путь к excel-файлу выгрузки из БД. По умолчанию - файл "DB_EXPORT.xlsx", находящийся в папке с программой. Возможно указывать путь, относительно данной папки.'
        },
        {
            'short': '-db_sheet_name',
            'full': '--db_export_sheet_name',
            'default': "Sheet 1",
            'required': False,
            'help': 'Название листа в файле выгрузки из БД. По умолчанию - "Sheet 1".'
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
            'short': '-s',
            'full': '--skip_errors',
            'action': 'store_true',
            'default': False,
            'required': False,
            'help': 'Пропускать ошибки при обработке адресов. По умолчанию - останавливаться при ошибке.'
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

    params = list(general_params)
    params.extend(excel_file_params)
    params.extend(db_params)

    parser = argparse.ArgumentParser()

    for param in params:
        short, full, = param.pop('short'), param.pop('full')
        parser.add_argument(short, full, **param)

    return parser