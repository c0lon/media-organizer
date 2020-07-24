# Media Organization Tool


## Usage

```bash
usage: organize [-h] --source SOURCES --target TARGET [-m {copy,link,move}]
                [--serial] [--dry-run]

Scan a directory to find, organize, and add media to the `/tank/media`
directory.

optional arguments:
  -h, --help            show this help message and exit
  --source SOURCES      move files from the given path
  --target TARGET       organize + move files to the given path
  -m {copy,link,move}, --mode {copy,link,move}
  --serial              run jobs serially
  --dry-run
```
