def check(scan_name,
          checks_subpath = None,
          data_source_name = 'portalinmobiliario',
          config_file = '/include/soda/configuration.yaml',
          checks_path = '/include/soda/checks'):
    from soda.scan import Scan

    print('Running Soda Data Check Scans...')

    if checks_subpath:
        checks_path += f'/{checks_subpath}'
    
    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source_name)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    if result != 0:
        raise ValueError('Soda Scan Failed')

    return result