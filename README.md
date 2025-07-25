# sensor project - golang

1. Install PostgreSql on Ubuntu 22.04
```
# sudo apt install postgresql postgresql-contrib
# sudo -i -u postgres
$ psql
ALTER USER postgres PASSWORD 'strongpassword';
```
2. Install packages
```
# wget https://golang.org/dl/go1.23.10.linux-amd64.tar.gz
# sudo tar -C /usr/local -xzf go1.23.10.linux-amd64.tar.gz
# echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
# source ~/.bashrc
# go version

# go get golang.org/x/sys/unix
# go get github.com/lib/pq
```
3. Prepare Project
```
# go mod init tempreg
# go mod tidy
```
4. Build Project
```
# go build
```
5. Run
```
# ./tempreg -loglevel=Debug contscan3min.cfg
```
(*) loglevel - Debug, Info, Warn, Error
    Default - Info

