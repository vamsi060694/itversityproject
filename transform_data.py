import pandas as pd
from pp.utils.logging_init import init_logger
from pp.etl import lookup_tables
import logging
from pp.etl.extraction import extraction, data_path, field, transform_path
init_logger()

extracted_data = extraction(data_path, field, transform_path)


def transforming_data(extracted_data):
    try:
        units_table = lookup_tables.get_unit_of_measure()
        energy_product_table = lookup_tables.get_energy_units()
        well_lookup_df = lookup_tables.get_all_wellids()
        extracted_data.columns = ['well_name', 'Month', 'crude_oil(m3)', 'natural_gas(km3)', 'other(m3)']
        transpose_data = pd.melt(extracted_data, id_vars=['well_name', 'Month'],
                                 value_vars=['crude_oil(m3)', 'natural_gas(km3)', 'other(m3)'], var_name='Comodity',
                                 value_name='Value')
        transpose_data[['energy', 'units']] = transpose_data['Comodity'].str.split('(', expand=True)
        transpose_data['units'] = transpose_data['units'].str.replace('[)]', '', regex=True)
        transpose_data['energy_product_id'] = transpose_data.energy.str.lower().map(energy_product_table.set_index('energy_product')['id'])
        transpose_data['energy_unit_id'] = transpose_data.units.str.lower().map(units_table.set_index('uom')['unit_of_measure_id'])
        transpose_data['well_id'] = transpose_data.well_name.map(well_lookup_df.set_index('well_name')['id'])
        transpose_data['month'] = transpose_data['month'].dt.strftime('%Y-%m-%d')
        transpose_data['value'] = transpose_data['value'].str.replace(',', '').astype(float)
        transpose_data = transpose_data.drop(['energy', 'units', 'Commodity', 'well_name'], axis=1)
        return transpose_data
    except Exception as e:
        logging.error(e)
