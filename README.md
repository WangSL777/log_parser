# log_parser
The python project is a work result for Binance QA Engineer Assessment. It will parse a log file and generate a result summary.


### Question from Assessment
3. What summarized or aggregated statistics (such as order requests per second) can be determined from
the information in the attached log file.

### How to run it
Can use command `python run.py -h` to check the command options, this is an example.
```
$ python run.py -h
usage: run.py [-h] [-f FILE_TO_PARSE] [--figure] [--clean] [-l {ERROR,WARN,INFO,DEBUG}]

options:
  -h, --help            show this help message and exit
  -f FILE_TO_PARSE, --file-to-parse FILE_TO_PARSE
                        The file path to be parsed, default to "logfile.log".
  --figure              generate figures after parsing the log file.
  --clean               remove existing log and parsed result before testing
  -l {ERROR,WARN,INFO,DEBUG}, --log-level {ERROR,WARN,INFO,DEBUG}
                        set the logging level, default to "INFO" level.

```
One example command
```
python run.py --clean -l DEBUG --figure -f logfile.log
```

### What it generates
The python script will create `generate` folder under current directory. After running the script, it will generate:
- result.json: present detailed aggregated statistics
- figure.html: the figure file to present 1. order request count over time, 2. rejected order request count over time
- figure.png: same as figure.html but in image file
- log_parser.log: the log file for this script

### Example generated files
- [This](https://mega.nz/file/U2lBUaSY#Yck7HsY8Z46ib8AUP_IRg7i_R3zJd6QOoSaVR5Q-5K8) is a video to show how to run it (Suggest using Chrome to watch it online)
- [These](https://mega.nz/folder/539wnLiK#sOkxUrS9XQ_jXwX5fUVT3A) are sample files that the script generates

