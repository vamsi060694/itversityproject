from pp.utils.db_connectivity import db_connection
import pandas as pd
from pp.utils.logging_init import init_logger
import sqlalchemy

logger = init_logger()


def update_well_table(well_names, field):
    try:
        wells_list = well_names.drop_duplicates(subset=['Well Name'], ignore_index=True)
        fields = fieldsdb_df()
        wells_list['field_id'] = wells_list['Field Name'].map(fields.set_index('field_name')['id'])
        wells_list = wells_list.drop(columns='Field Name')
        fields_df_q = fields[fields['field_name'] == field]['id'].values[0]

        wells_list = wells_list[['Well Name', 'field_id']]
        wells_list.columns = ['well_name', 'field_id']
        wells_list['flag'] = "file"

        wellsdb_check_df = get_well_ids(fields_df_q)
        wellsdb_check_df = wellsdb_check_df[['well_name', 'field_id']]
        wellsdb_check_df['flag'] = 'db'
        well_filter_names = pd.concat([wells_list, wellsdb_check_df]).drop_duplicates(subset=['well_name', 'field_id'],
                                                                                      keep=False)
        well_filter_names = well_filter_names[well_filter_names['flag'] == 'file']
        well_filter_names = well_filter_names.drop(columns=['flag'])
        logger.info("Number of new wellnames into lookup table {}".format(len(well_filter_names)))

        if len(well_filter_names) > 0:
            logger.info('Inserting new wells energy_etl into lookup table')
            conn = db_connection()
            well_filter_names.to_sql(
                'tbl_cnlopb_wells',
                con=conn,
                if_exists='append',
                method='multi',
                index=False,
                schema='collections',
                dtype={"well_name": sqlalchemy.types.VARCHAR(length=30),
                       "field_id": sqlalchemy.types.CHAR(length=5)
                       }
            )
        else:
            logger.info("All wells are already exists in lookup table")
    except Exception as e:
        logger.error(e)


def get_well_ids(field_id):
    try:
        conn = db_connection()

        sql = f"select id, field_id, well_name from collections.tbl_cnlopb_wells where field_id = '{field_id}'"
        wellsdb_df = pd.read_sql(con=conn, sql=sql)
        logger.info('Returned the available Well table successfully')
    except Exception as e:
        logger.error(e)
    return wellsdb_df


def get_all_wellids():
    try:
        conn = db_connection()

        sql = f"select id, field_id, well_name from collections.tbl_cnlopb_wells"
        wellsdb_df = pd.read_sql(con=conn, sql=sql)

        logger.info('Returned the available Well table successfully')
    except Exception as e:
        logger.error(e)
    return wellsdb_df


def update_fields_table(fieldname):
    try:
        conn = db_connection()
        sql = 'select * from collections.tbl_cnlopb_fields'
        field_table = pd.read_sql(sql=sql, con=conn)
        if fieldname in list(field_table.field_name.values):
            logger.info("{} field is already there".format(fieldname))
        else:
            sql = f"insert into collections.tbl_cnlopb_fields (field_name) values ('{fieldname}')"
            conn.execute(sql)
            logger.info(f"Inserted new field {fieldname} into table")
    except Exception as e:
        logger.error(e)


def fieldsdb_df():
    try:
        conn = db_connection()
        sql = 'select ID, field_name from collections.tbl_cnlopb_fields'
        fields_db = pd.read_sql(con=conn, sql=sql)
        logger.info("Returned the fields energy_etl")
    except Exception as e:
        logger.error(e)
    return fields_db


def get_energy_units():
    try:
        conn = db_connection()

        sql = 'select ID,energy_product from metadata.tbl_energy_product'
        tbl_energy_product_df = pd.read_sql(con=conn, sql=sql)
        logger.info("Returned the energy product energy_etl")
    except Exception as e:
        logger.error(e)
    return tbl_energy_product_df


def get_unit_of_measure():
    try:
        conn = db_connection()

        sql = 'select id,unit_short from metadata.tbl_unit'
        tbl_uom_df = pd.read_sql(con=conn, sql=sql)
        tbl_uom_df.columns = ['unit_of_measure_id','uom']
        logger.info("Returned the units energy_etl ")
    except Exception as e:
        logger.error(e)
    return tbl_uom_df