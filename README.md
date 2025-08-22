# Plugin QA

## Testing datasets

This Dataiku DSS plugin provides a connector to generate a dataset for plugin testing purpose. This dataset can be fed to the plugin to test, and the plugin's output can be analyzed using the provided recipe. A report dataset is produced counting the number of errors.

![](img/plugin%20testing%20flow.png)

## Testing file system

The *File system testing* recipe runs common file operation tests on any managed folder
- File / folder+file creation
- Folder lising and *last modified* / *file size* testing
- File / folder+file deletion

![](img/fs%20testing%20flow.png)

### Change logs

- v0.0.1 Initial version
- v0.0.2 adding date, datetime with tz, datetime no tz, schema export
- v0.0.3 adding filesystem testing

### Licence

Copyright 2020-2022 Dataiku SAS

This plugin is distributed under the Apache License version 2.0
