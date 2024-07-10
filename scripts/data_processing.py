from typing import Literal, Self
import csv
import json


class Data:
    def __init__(self, data: list[dict]) -> None:
        self.data = data
        self.column_names = self.__get_columns()
        self.count_lines = self.__get_size_data()

    def __read_json(path) -> list[dict]:
        with open(path, 'r') as file:
            return json.load(file)

    def __read_csv(path) -> list[dict]:
        data_csv = []
        with open(path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                data_csv.append(row)

            return data_csv

    @classmethod
    def read_data(cls, path: str, type: Literal['csv', 'json']) -> Self:
        data = []
        if type == 'csv':
            data = cls.__read_csv(path)

        elif type == 'json':
            data = cls.__read_json(path)

        return cls(data)

    def __get_columns(self) -> list[str]:
        return list(self.data[-1].keys())

    def rename_columns(self, key_mapping: dict) -> list[dict]:
        self.data = [
            {key_mapping[old_key]: value for old_key,
                value in old_dict.items()}
            for old_dict in self.data
        ]
        self.column_names = self.__get_columns()

    def __get_size_data(self):
        return len(self.data)

    @classmethod
    def join(cls, data_a: Self, data_b: Self) -> Self:
        combined_data_list = []
        combined_data_list.extend(data_a.data)
        combined_data_list.extend(data_b.data)
        return cls(combined_data_list)

    def __transform_data_table(self) -> list[str]:
        combined_data_table = [self.column_names]
        combined_data_table.extend([
            [row.get(column, 'Indisponivel') for column in self.column_names] for row in self.data
        ])
        return combined_data_table

    def save_data(self, path: str) -> None:
        combined_data_table = self.__transform_data_table()
        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(combined_data_table)
        print(f'Save data successful in "{path}"')
