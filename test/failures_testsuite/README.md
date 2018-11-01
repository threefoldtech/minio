# failure test suite to cover all failure scenarios
## Pre-requistes:
- ubuntu 16.04
- js (development) 
- zrobot (development)
- `mc --vesrsion || wget https://dl.minio.io/client/mc/release/linux-amd64/mc -O /bin/mc && chmod +x /bin/mc`

## steps:
```
git clone https://github.com/threefoldtech/minio.git
cd test/failures_testsuite
vim config.yaml #update it
pip3 install -r requirements.txt
nosetests-3.4 -vs --logging-level=WARNING testcases
```