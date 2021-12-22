import os
from pp.etl import lookup_tables, production_data
from pp.etl.Downloading_the_data_files import downloading_src_files, data_path, filters
from pp.etl.extraction import extraction
from pp.etl.transform_data import transforming_data, transform_path
from pp.utils.logging_init import init_logger
import json
import logging

init_logger()


def data_insert():
    try:
        downloading_src_files(filters, data_path)
        for filter in filters:
            field = json.loads(os.getenv('FIELDS'))[filter]
            extracted_data = extraction(data_path, field, transform_path)
            extracted_data['Field Name'] = field
            lookup_tables.update_fields_table(field)
            well_df = extracted_data[['Well Name', 'Field Name']]
            lookup_tables.update_well_table(well_df, field)
            transformed_data = transforming_data(extracted_data)
            production_data.production_update_table(transformed_data)
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    data_insert()












