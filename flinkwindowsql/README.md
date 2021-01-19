Flink提供了几种常用的watermark策略。

1,严格意义上递增的时间戳,发出到目前为止已观察到的最大时间戳的水印。时间戳小于最大时间戳的行不会迟到。

WATERMARK FOR rowtime_column AS rowtime_column

2,递增的时间戳,发出到目前为止已观察到的最大时间戳为负1的水印。时间戳等于或小于最大时间戳的行不会迟到。

WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND.

3,有界时间戳(乱序) 发出水印，它是观察到的最大时间戳减去指定的延迟，
    例如，WATERMARK FOR rowtime_column AS rowtime_column-INTERVAL'5'SECOND是5秒的延迟水印策略。

WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit.



5,format.type目前支持的数据结构主要有三种,'csv', 'json' and 'avro'.

6,connector.version,在kafka0.11以上的版本可以写universal表示通用.