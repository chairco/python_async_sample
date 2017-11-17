## Nikon ROT ETL Project Setting

### Requirements library

+ optparse: Management  argument
+ logging: log stdout management
+ dplyr: data filter, select etc
+ reshape2: melt() and dcast(), output data.frame()
+ ROracle: connect Oracle
+ RPostgreSQL: connect PostgreSQL

### Enviroment

cp env_sample.R to env.R and completed varable project necessary

```
cp env_sample.R env.R
```

### How to work

show help how to use.
```
Rscript rot.R  # ROT
Rscript mea.R  # ROT mea
```

Shift and rotate the Nikon TP data between a time interval.
```
Rscript rot.R -t 0501 -s 2017-07-13 08:00:00 -e 2017-07-14 08:00:00
```


Shift and rotate the measurement data between a time interval. mea no need toolid parameter.
```
Rscript mea.R -s 2017-07-13 08:00:00 -e 2017-07-14 08:00:00
```



