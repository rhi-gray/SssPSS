import pyreadstat as readstat
import pandas as pd

from datetime import datetime, timedelta
import re

def spss_format(fmt, value):
    fmt = fmt.upper()
    matches = re.match(r"([A-Z]+)(\d+)(\.(\d+))?", fmt)
    TYPE, WIDTH, _, DEC = matches.groups()
    if DEC is None: DEC = 0

    # String type.
    if TYPE == "A":
        return "{:>%Ls}".replace("%L", WIDTH).format(value)

    # Regular Numeric type.
    elif TYPE == "F":
        return "{:%U.%Df}".replace("%U", WIDTH).replace("%D", DEC).format(value)

    elif TYPE == "PCT":
        return "{:%U.%Df}%".replace("%U", WIDTH).replace("%D", DEC).format(value)

    # Dollar values.
    elif TYPE == "DOLLAR":
        return "${:%U.%Df}".replace("%U", WIDTH).replace("%D", DEC).format(value)
    
    # Datetimes are stored as a number of seconds since 1582-01-01 00:00
    elif TYPE == "DATETIME": # fmt.startswith("DATETIME"):
        offset = datetime(1582, 1, 1)
        value_dt = offset + timedelta(seconds=value)
        match fmt:
            case "DATETIME17": return value_dt.strftime("%d-%b-%Y %H:%M")
            case "DATETIME20": return value_dt.strftime("%d-%b-%Y %H:%M:%S")
            case "DATETIME22": return value_dt.strftime("%d-%b-%Y %H:%M:%S.%f")[:23]

    # Dates are converted to python datetime.date objects
    elif TYPE in ("DATE", "EDATE", "ADATE", "SDATE"):
        match TYPE:
            # DATE - 22-Feb-2022
            case "DATE": 
                return value.strftime("%d-%b-%Y")
            # EDATE - 22.02.2022
            case "EDATE": 
                return value.strftime("%d.%m.%Y")
            # ADATE - 02/22/2022
            case "ADATE":
                return value.strftime("%m/%d/%Y")
            # SDATE - 2022/02/22
            case "SDATE": 
                return value.strftime("%Y/%m/%d")
            # else:
            case _:
                return value.strftime("%Y-%m-%d")

    elif TYPE in ("TIME", "MTIME", "DTIME"):
        # Times are converted to python datetime.time objects
        match fmt:
            # HH:MM
            case "TIME4" | "TIME5": return value.strftime("%H:%M")
            # HH:MM:ss
            case "TIME8": return value.strftime("%H:%M:%S")

    else:
        return str(value)

class SavColumn:
    def __init__(self, data,
                 col_name: str = "column name",
                 col_label: str = "column label",
                 variable_type: str = "F8.2",
                 val_labels: dict = {}):
        self.name = col_name
        self.label = col_label if col_label is not None else ''
        self.data = data
        self.type = variable_type
        self.value_labels = val_labels

    def _print(self, use_value_labels = True, head = -1, tail = -1):
        """ Print user-friendly info about a column.
        Dumps some metadata, then some of the data.
        """
        val_labels = '\n\t'.join(f'{code}\t=> "{label}"'
                                  for code, label in self.value_labels.items())
 
        # If it has value labels: use them
        def fmt(cell):
            return self.value_labels.get(cell, spss_format(self.type, cell))

        # Show the first `head` rows.
        if head != -1:
            data = "\n".join(f"\t{fmt(cell)}" for cell in self.data.iloc[:head]) + "\n\t…"
        # ... Or the last `tail` rows.
        elif tail != -1:
            data = "\n\t…" + "\n".join(f"\t{fmt(cell)}" for cell in self.data.iloc[-tail:])
        else:
            # For short datasets, show the whole lot.
            if len(self.data) < 10:
                data = "\n".join(f"\t{fmt(cell)}" for cell in self.data)
            # Otherwise, show the first 5 and last 3.
            else:
                data = "\n".join(f"\t{fmt(cell)}" for cell in self.data.iloc[:5])
                data += "\n\t…\n"
                data += "\n".join(f"\t{fmt(cell)}" for cell in self.data.iloc[-3:])

        ret = ""
        ret += f"Column name:\t{self.name}\n"
        ret += f"Column label:\t'{self.label}'\n"
        ret += f"Format:\t{self.type}\n"
        if val_labels:
            ret += "Value labels:\n"
            ret += f"\t{val_labels}\n"
        else:
            ret += "Value labels:\t(none)\n"

        ret += f"Data (n={len(self.data)} cases):\n" + data

        return ret

    def __str__(self):
        return self._print(use_value_labels = True)

    def __repr__(self):
        return self._print(use_value_labels = False)

    def head(self, n = 10):
        return self._print(head = n)
    def tail(self, n = 10):
        return self._print(tail = n)

    def __getitem__(self, key):
        """ Access cases with [] notation. """
        return self.data.iloc[key]

    def __setitem__(self, key, value):
        self.data.iloc[key] = value

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return SavColumnIterator(self)

    def attach(self):
        import builtins
        if self.name in dir(builtins):
            print(f"Error: won't overwrite builtin name '{self.name}'!")
        else:
            setattr(builtins, self.name, self)

class SavColumnIterator:
    def __init__(self, data):
        self.cursor = 0
        self.data = data
    
    def __next__(self):
        self.cursor = self.cursor + 1
        if self.cursor > len(self.data):
            raise StopIteration
        else:
            return self.data.iloc[self.cursor - 1]

class SavFile:
    def __init__(self, filepath: str = ""):
        self.df, self._meta = readstat.read_sav(filepath)
        
        self.columns = {}
        for col_name in self._meta.column_names:
            label = self._meta.column_names_to_labels.get(col_name, "")
            var_type = self._meta.original_variable_types.get(col_name, "F8.2")
            val_labels = self._meta.variable_value_labels.get(col_name, {})
            self.columns[col_name] = SavColumn(self.df[col_name],
                                              col_name,
                                              label,
                                              var_type,
                                              val_labels)

    def __str__(self):
        return f"SPSS .sav\nn={len(self.df)} cases\n\n" + "\n".join(
                self.columns[col].head() for col in self.columns)

    def __repr__(self):
        return str(self)

    def __getitem__(self, key, default = None):
        """ Either get a column, or a case (row):
            SavFile["colname": str] => SavColumn 
            SavFile[index: int] => SavRow
        """
        if isinstance(key, str):
            return self.columns.get(key, default)
        elif isinstance(key, int):
# TODO: Implement SavRow and SavFile[index]
            raise NotImplemented("SavRow not yet implemented!")

    def __len__(self): return self.nrows()

    def nrows(self):
        """ Return the number of rows in this SavFile. """
        return len(self.df)

    def ncols(self):
        """ Return the number of columns in this SavFile. """
        return len(self.columns)

    def rows(self):
        """ Iterate over the rows in this SavFile.

        Returns a generator.
        """
# TODO: Implement SavRow and SavFile.rows()
        raise NotImplemented("SavRow not yet implemented!")

    def cols(self):
        """ Iterate over the columns in this SavFile. 

        Returns a generator.
        """
        for col_name in self.columns:
            yield self.columns[col_name]

    def attach(self):
        """ Expose this SavFile's columns to the global scope for iteration. """
        for column in self.columns.keys():
            self.columns[column].attach()

def load(filepath: str) -> SavFile:
    """ Return a SavFile with data frame and metadata info. """
    return SavFile(filepath)
