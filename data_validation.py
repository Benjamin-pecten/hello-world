#This is the generic data validation class.

import json
from pecten_utils.Storage import Storage
import pandas as pd
import holidays
from datetime import datetime
import jsonpickle

class PectenDataValidator:
    def __init__(self):
        pass

    @classmethod
    def validate(cls,args,data, rules=[], **kwargs):
        """
        This function performs data validation and saves invalid data records
        to a table. It works as a sort of filter for the calling script, by
          not allowing invalid data to be inserted into the database.
          It receives a set of data, performs validations on each record and
          on the whole set, and returns a tuple with the valid and invalid
           data to the calling script.

        First, the function will perform some generic data validation rules
        and then it will perform the data validation rules supplied as an
        argument.

        Args:
            - data (string): JSON string containing the data records to
            validate. It should be of the form:
                '{"data":[
                            {"column1":"value1",
                            "column2":"value2",
                            "column3":"value3"
                            ...},
                            {"column1":"value1",
                            "column2":"value2",
                            "column3":"value3"
                            ...},
                        ]

                }'

            - rules (list): list of functions, where each function takes a JSON string
          containing the data records to validate, performs a validation
            rule and returns a tuple, where the first element is the collection of valid
          records, and the second element is the collection of invalid records. These
          records should be returned as lists of
          dict with the data; and in the case of invalid records, the data will be a JSON
          String, and the dict will be
          augmented with the reason, the rule, the script, and the time of
          Validation. For example, the collection of invalid records might look like this:

          [{"data":'{"column1":"value1"...}', "rule":rule_value,
          "reason":reason_value, "script":script_value,
          "date_of_validation":date_value
          },
          {"data":'{"column1":"value1"...}', "rule":rule_value,
          "reason":reason_value, "script":script_value,
          "date_of_validation":date_value
          },
          ...
          ]

          The collection of valid records will be simpler, just a list of dict, e.g.
          [{"column1":value1, "column2":value2},
           {"column1":value1, "column2":value2}
          ]

            - kwargs:
                - 'google_key_path': the path to the service account key.

        Returns:
            tuple: tuple with: a list of dict with the valid records as the first element,
            and a list of dict with the invalid records as the second element.
            Each element of the invalid list is a tuple, containing the actual data (dict) as its first element,
            and another dict as its second element, with additional information like rule, reason, script,
            date_of_calidation and last_update_date. Exmple:

            Validate would return:

            (valid_list,invalid_list)

            where valid_list is:
            [
              {"column1":value1, "column2":value2...},
              {"column1":value1, "column2":value2...},
              ...
            ]

            where invalid_list:

            [
             (valid_record, additional_info),
             (valid_record,additional_info),
             ...
            ]

            where valid_record is:

            {"column1":value1, "column2":value2...}

            where additional_info is:

            {"rule":rule_value,
             "reason":reason_value,
             "script":script_value,
             "date_of_validation":date_of_validation_value,
             "last_update_date":last_update_date_value
             }

        """
        def rule_1(row, tc, ntc, uk, de, script):
            for c in ntc:
                if row[c] is not None and row[c] != 0:
                    return "valid"

            #All values were 0
            #Check if they are holiday or weekend
            for c in tc:
                try:
                    parsed = datetime.strptime(row[c], '%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    continue
                parsed_date = str(parsed.date())
                if parsed_date in uk or parsed_date in de or not parsed.weekday():
                    if "price" in script:
                        return "valid"
                    else:
                        rule_list.add("rule_1")
                        return "invalid"

            #Not weekend or holiday
            rule_list.add("rule_1")
            return "invalid"

        def rule_4(row,nsc):
            for c in nsc:
                if row[c] != 0 and row[c] is not None:
                    return "valid"

            rule_list.add("rule_4")
            return "invalid"

        data_decoded = jsonpickle.decode(data)
        valid_data = []
        invalid_data = []

        rule_list = set()
        reasons = {
            "rule_1": "Rule 1: all columns (except timestamp) are null or 0.",
            "rule_2": "Rule 2: the 4000 previously-validated records are identical.",
            "rule_3": "Rule 3: the same column had value of 0 or NULL for the last 5 rows.",
            "rule_4": "Rule 4: standard columns like Constituent name, id, date have values but all others are 0."
        }

        #First, perform generic data validation rules
        #Rule 1: check if all columns (except timestamp) are null or 0
        #Get timestamp and non-timestamp columns
        script = kwargs["script"]
        uk_holidays = holidays.UK()
        de_holidays = holidays.Germany()
        df = pd.DataFrame(data_decoded["data"])
        standard_columns = ['constituent_name', "constituent_id", "date", "last_update_date"]
        original_columns = df.columns
        timestamp_columns = [c for c in df.columns if ("date" in c or "time" in c)]
        non_timestamp_columns = [c for c in df.columns if ("date" not in c and "time" not in c)]
        non_standard_columns = [c for c in df.columns if c not in standard_columns + timestamp_columns]

        df["rule_1"] = df.apply(lambda x: rule_1(x,timestamp_columns,non_timestamp_columns,uk_holidays,
                                                 de_holidays,script), axis=1)

        #Rule 2: Are the 4000 previously-validated records identical?
        invalid_indices = set()
        if df.shape[0] >= 4000:
            #We can apply rule
            start = 0
            end = 3999
            while end < df.shape[0]:
                df_temp = df.loc[start:end]
                df_duplicates = df_temp[df_temp.duplicated(subset=non_timestamp_columns, keep=False)]
                invalid_indices.update(list(df_duplicates.index))

                start += 1
                end += 1

        df["rule_2"] = "valid"
        df.loc[list(invalid_indices), "rule_2"] = "invalid"

        if len(invalid_indices) > 0:
            rule_list.add("rule_2")

        #Rule 3: Has the same column had value of 0 or NULL for the last 5 rows?
        invalid_indices_2 = set()
        start = 0
        end = 4
        while end < df.shape[0]:
            for c in non_timestamp_columns:
                series = df.loc[start:end][c]
                if series.any() == False:
                    invalid_indices_2.update(list(series.index))

            start += 1
            end += 1

        df["rule_3"] = "valid"
        df.loc[list(invalid_indices_2), "rule_3"] = "invalid"

        if len(invalid_indices_2) > 0:
            rule_list.add("rule_3")

        #Rule 4: If standard columns like Constituent name, id, date have values but all others are 0 reject?

        df["rule_4"] = df.apply(lambda x: rule_4(x,non_standard_columns), axis=1)

        df["rule"] = object

        #Get invalid records
        invalid_indices = set()

        for i in range(0, df.shape[0]):
            row_rules = []
            row = df.iloc[i]

            for r in ["rule_1","rule_2", "rule_3", "rule_4"]:
                if row[r] == "invalid":
                    invalid_indices.add(i)
                    row_rules.append(r)

            df.at[i, 'rule'] = row_rules

        #Get valid indices
        valid_indices = set(df.index.tolist())
        valid_indices = valid_indices.difference(invalid_indices)

        valid_data += df.loc[list(valid_indices)][original_columns.tolist() + ["rule"]].to_dict(orient='records')
        invalid_data += df.loc[list(invalid_indices)][original_columns.tolist() + ["rule"]].to_dict(orient='records')

        #Custom rules
        custom_invalid = []
        for func in rules:
            valid, invalid = func(jsonpickle.encode({"data":valid_data}))
            #print(invalid)
            #Check if valid
            valid_data = valid
            custom_invalid += invalid

        #Format invalid data
        invalid_data_store = []

        #Add rule and reason
        for item in invalid_data:
            if "rule_1" in item["rule"] or "rule_4" in item["rule"]:
                continue
            additional_info = {}
            additional_info["rule"] = item["rule"]
            additional_info["reason"] = [reasons[r] for r in item["rule"] if isinstance(item["rule"], list)]
            additional_info["script"] = script
            additional_info["date_of_validation"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            additional_info["last_update_date"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            invalid_data_store.append((item,additional_info))

        invalid_data_store += custom_invalid

        return list(valid_data), invalid_data_store


#========================#
#Examples
#=======================#
'''
kwargs = {}
kwargs['script'] = "test"

#Rule 2:
v = [{"constituent_id":"a", "date": "2018-12-21 00:00:00", "field":1}] * 2000
v += [{"constituent_id":"b", "date": "2018-12-21 00:00:00", "field":1}] * 2000
data2_v = {"data":v}

i = [{"constituent_id":"a", "date": "2018-12-21 00:00:00", "field":1}] * 4000
data2_i = {"data":i}

print(PectenDataValidator.validate(None,json.dumps(data2_v), **kwargs))
print(PectenDataValidator.validate(None,json.dumps(data2_i), **kwargs))

#Rule 3:
v = [{"constituent_id":"a", "date": "2018-12-21 00:00:00", "field":1}] + \
    [{"constituent_id":"b", "date": "2018-12-21 00:00:00", "field":2}] * 4

data3_v = {"data":v}

i = [{"constituent_id":None, "date": "2018-12-21 00:00:00", "field":0}] * 5
data3_i = {"data":i}

print(PectenDataValidator.validate(None,json.dumps(data3_v), **kwargs))
print(PectenDataValidator.validate(None,json.dumps(data3_i), **kwargs))

#Rule 4:
v = [{"constituent_id":"a", "date": "2018-12-21 00:00:00", "field":1}] * 3

data4_v = {"data":v}

i = [{"constituent_id":"a", "date": "2018-12-21 00:00:00", "field":0}] * 3
data4_i = {"data":i}

print(PectenDataValidator.validate(None,json.dumps(data4_v), **kwargs))
print(PectenDataValidator.validate(None,json.dumps(data4_i), **kwargs))

#Custum rules
def rule_6(data):
    data = (json.loads(data))["data"]
    valid = []
    invalid = []
    for item in data:
        if item["field1"] == item["field2"]:
            valid.append(item)
        else:
            invalid.append({"data":json.dumps(item),
                            "reason":"field1 == field2",
                            "rule":"rule_6",
                            "date":datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "last_update_date":datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    return valid,invalid

data_c = {"data":[
    {"field1":1,"field2":1},
    {"field1":2, "field2":2},
    {"field1":"a", "field2":"b"},
    {"field1":None,"field2":None}
]}

print(PectenDataValidator.validate(None,json.dumps(data_c),[rule_6], **kwargs))
'''