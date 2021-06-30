# shadowsocks-pbf

Persistent bloom filter for shadowsocks. 

It is mainly with reference to the mechanism of AOF (Append Only File) of redis.

However, there is still the problem of occupying memory. Visit [disk-bloom-filter](https://github.com/mzz2017/disk-bloom-filter) for another solution.

## Thanks

+ This project was born after discussed with [rprx](https://github.com/rprx).
+ Bloom filter is from [go-shadowsocks2](https://github.com/shadowsocks/go-shadowsocks2/blob/master/internal)
  and [go-bloom](https://github.com/riobard/go-bloom/blob/master/filter.go).

