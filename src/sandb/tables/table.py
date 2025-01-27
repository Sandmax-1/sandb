import json
from typing import Any, Sequence

from sandb.tables.metadata import TableMetadata


class TableExistsError(Exception):
    ...


class RowTypeError(Exception):
    """Error to raise when trying to insert a row to table with the wrong dtypes"""

    ...


class Table:
    def __init__(self, metadata: TableMetadata) -> None:
        self.metadata = metadata

    def create(self) -> None:
        """
        Creates a "table" folder. Within this folder we have two files.
        A json file called metadata.json: this will store the contents of
        the metadata TableMetadata object and tell us how to access the other file.
        The other file is called data.csv. It is a csv that we can get the type info
        for from the metadata file. Each row in the csv will correspond to a row in
        the table.

        Args:
            metadata (TableMetadata): Contains all the metadata for the given table.
            I.e. name, columns, datatypes, e.t.c

        Raises:
            TableExistsError: Raised if we are trying to create a table that
                              already exists. i.e. a folder of metadata.name
                              already exists.
        """
        table_path = self.metadata.location / self.metadata.name

        if table_path.exists():
            raise TableExistsError(
                f"Table already exists at location: {self.metadata.location}"
            )

        table_path.mkdir()
        self.metadata.data_path.touch()

        with open(self.metadata.metadata_path, "w") as f:
            json.dump(self.metadata.model_dump_json(), f)

    def write(self, row: Sequence[Any]) -> None:
        """
        Take a row of arbitrary data, ensure it has the correct shape and types,
        then save to file

        Args:
            row (tuple[Any, ...]): arbitrary tuple which we are trying to save
                                to the database
            table (TableMetadata): table metadata for the row we are trying to save.

        Raises:
            e: _description_
        """
        if self.validate_and_cast_row(row):
            with open(self.metadata.data_path, "a") as f:
                row_string = ", ".join(map(str, row))
                f.write(row_string + "\n")

        else:
            raise RowTypeError(f"Failed to write row {row} due to type mismatch.", row)

    def validate_and_cast_row(self, row: Sequence[Any]) -> tuple[Any, ...]:
        """
        Validate and cast the row's elements to their corresponding
        types as defined in the table.

        Args:
            row (Iterable[Any]): The input row to validate and cast.
            table (TableMetadata): Metadata of the table with the expected types.

        Returns:
            tuple: The validated and casted row.

        Raises:
            RowTypeError: If the row's types or length don't match the table.
        """
        if len(row) != len(self.metadata.dtypes):
            raise RowTypeError(
                f"""Row length {len(row)} doesn't
                    match expected length {len(self.metadata.dtypes)}."""
            )

        try:
            return tuple(
                dtype(element)
                for dtype, element in zip(self.metadata.dtypes, row, strict=True)
            )
        except ValueError as e:
            raise RowTypeError(f"Type mismatch for row {row}.") from e

    def read(self, column_to_query: str, predicate: Any) -> list[tuple[Any]]:
        """
        Currently just performs a full table scan reading row by row from the data.csv
        file pointed to from TableMetadata.
        Can only perform WHERE column_to_query == predicate. Will add more functionality
        in the future.

        Args:
            column_to_query (str): Column to check
            predicate (Any): value to check column against
            table (TableMetadata): table to scan

        Returns:
            list[tuple[Any]]: a list of all rows that match the
                              column_to_query == predicate
        """
        try:
            col_position = self.metadata.col_names.index(column_to_query)
        except ValueError as e:
            raise ValueError(f"{column_to_query} not in {self.metadata}") from e

        out: list[tuple[Any, ...]] = []
        with open(self.metadata.data_path, "r") as f:
            for line in f.readlines():
                element_list = line.strip().split(", ")
                typed_row = self.validate_and_cast_row(element_list)
                if typed_row[col_position] == predicate:
                    out.append(typed_row)

        return out

    # def join(self, column_to_join_on: str, table_to_join_to: "Table"):
    #     column_index_left = self.metadata.col_names.index(column_to_join_on)
    #     column_index_right = table_to_join_to.metadata.col_names.index(
    #         column_to_join_on
    #     )

    #     left_data = self.read(column_to_join_on)
    #     right_data = table_to_join_to.read(column_to_join_on)
