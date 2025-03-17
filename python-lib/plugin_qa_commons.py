from time import strftime, localtime


class RecordsLimit():
    def __init__(self, records_limit=-1):
        self.has_no_limit = (records_limit == -1)
        self.records_limit = records_limit
        self.counter = 0

    def is_reached(self):
        if self.has_no_limit:
            return False
        self.counter += 1
        return self.counter > self.records_limit


def build_row(row_number, number_of_columns, use_cjk=False, use_emoji=False, use_date=False, use_datetime_tz=False, use_datetime_no_tz=False):
    row = {}
    for column_number in range(0, number_of_columns):
        row.update(
            {
                build_column_name(column_number, use_cjk=use_cjk, use_emoji=use_emoji): chaos_monkey(
                    build_value(row_number, column_number,use_date=use_date, use_datetime_tz=use_datetime_tz, use_datetime_no_tz=use_datetime_no_tz)
                )
            }
        )
    return row


def build_column_name(column_number, use_cjk=False, use_emoji=False, column_prefix="col"):
    tokens = []
    tokens.append(column_prefix)
    tokens.append("{}".format(column_number + 1))
    if use_cjk:
        tokens.append(get_cjk(0, column_number))
    if use_emoji:
        tokens.append(get_emoji(0, column_number))
    return "_".join(tokens)


def build_value(row_number, column_number, use_date=False, use_datetime_tz=False, use_datetime_no_tz=False):
    cell_type = get_type(column_number, use_date=use_date, use_datetime_tz=use_datetime_tz, use_datetime_no_tz=use_datetime_no_tz)
    if cell_type == "string":
        return "{}_{}".format(row_number+1, column_number+1)
    if cell_type == "bigint":
        return row_number * column_number
    if cell_type == "double":
        return row_number / (column_number+1)
    if cell_type in ["dateonly", "datetimenotz", "date"]:
        epoch = row_number * 86400 + 40271 * column_number # One day + 11h 11m 11s per column
        if cell_type == "datetimenotz":
            return strftime('%Y-%m-%d %H:%M:%S', localtime(epoch))
        if cell_type == "date":
            return strftime('%Y-%m-%dT%H:%M:%S.000Z', localtime(epoch))
        if cell_type == "dateonly":
            return strftime('%Y-%m-%d', localtime(epoch))
    return None


def get_type(column_number, use_date=False, use_datetime_tz=False, use_datetime_no_tz=False):
    available_types = ["string", "bigint", "double"]
    if use_date:
        available_types.append("dateonly")
    if use_datetime_tz:
        available_types.append("date")
    if use_datetime_no_tz:
        available_types.append("datetimenotz")
    column_type_number = column_number % len(available_types)
    return available_types[column_type_number]


def chaos_monkey(cell_content):
    return cell_content


def get_cjk(row_number, column_number):
    samples = ["普通话", "한글", "韓㐎", "カタカナ", "ひらがな", "漢字"]
    return sample_picker(samples, (row_number + 1)*(column_number + 1))


def get_emoji(row_number, column_number):
    samples = ["😀", "🔥", "🐦‍🔥"]
    return sample_picker(samples, (row_number + 1)*(column_number + 1))


def sample_picker(samples, range):
    return samples[range % len(samples)]
