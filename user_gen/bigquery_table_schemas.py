def get_molecular_schema():
    return [
        {'name': 'sample_barcode',  'type': 'STRING'},
        {'name': 'project_id',      'type': 'INTEGER'},
        {'name': 'study_id',        'type': 'INTEGER'},
        {'name': 'Platform',        'type': 'STRING'},
        {'name': 'Pipeline',        'type': 'STRING'},
        {'name': 'Symbol',          'type': 'STRING'},
        {'name': 'ID',              'type': 'STRING'},
        {'name': 'Tab',             'type': 'STRING'},
        {'name': 'Level',           'type': 'FLOAT'}
    ]

# Not used
def get_user_gen_schema():
    return [
        {'name': 'sample_barcode',
         'type': 'STRING'},
        {'name': 'project_id',
         'type': 'INTEGER'},
        {'name': 'study_id',
         'type': 'INTEGER'}
    ]