def get_molecular_schema():
    return [
        {'name': 'SampleBarcode',
         'type': 'STRING'},
        {'name': 'Project',
         'type': 'INTEGER'},
        {'name': 'Study',
         'type': 'INTEGER'},
        {'name': 'Platform',
         'type': 'STRING'},
        {'name': 'Pipeline',
         'type': 'STRING'},
        {'name': 'Symbol',
         'type': 'STRING'},
        {'name': 'ID',
         'type': 'STRING'},
        {'name': 'Tab',
         'type': 'STRING'},
        {'name': 'Level',
         'type': 'FLOAT'}
    ]

def get_user_gen_schema():
    return [
        {'name': 'SampleBarcode',
         'type': 'STRING'},
        {'name': 'Project',
         'type': 'INTEGER'},
        {'name': 'Study',
         'type': 'INTEGER'}
    ]