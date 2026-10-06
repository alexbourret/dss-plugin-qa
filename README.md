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

## Chaos Monkey

The *Chaos Monkey* recipe takes one input dataset and writes one output dataset with the same schema. Columns are processed in schema order.

- **Use all dataset** copies every input row. The first row is unchanged; the second row has its first column set to null, the third row has its second column set to null, and so on through the last column. Any remaining rows are unchanged. Short datasets only null the columns reached by their available rows.
- **Duplicate first line** uses only the first input row. It writes an unchanged copy, then one copy per column with only that column set to null, then another unchanged copy. For an input with N columns, the output contains N + 2 rows.

An empty input produces an empty output in either mode. Existing null values are preserved.

### Change logs

- v0.0.1 Initial version
- v0.0.2 adding date, datetime with tz, datetime no tz, schema export
- v0.0.3 adding filesystem testing

### Licence

Copyright 2020-2022 Dataiku SAS

This plugin is distributed under the Apache License version 2.0
