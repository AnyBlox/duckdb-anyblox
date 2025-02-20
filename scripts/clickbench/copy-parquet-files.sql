COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid <= 25006080) TO '/home/gienieczko/hdd/hits-strings-0-uncompressed.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION uncompressed);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 25006080 AND rid <= 50012160) TO '/home/gienieczko/hdd/hits-strings-1-uncompressed.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION uncompressed);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 50012160 AND rid <= 75018240) TO '/home/gienieczko/hdd/hits-strings-2-uncompressed.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION uncompressed);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 75018240) TO '/home/gienieczko/hdd/hits-strings-3-uncompressed.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION uncompressed);

COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid <= 25006080) TO '/home/gienieczko/hdd/hits-strings-0-snappy.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION snappy);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 25006080 AND rid <= 50012160) TO '/home/gienieczko/hdd/hits-strings-1-snappy.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION snappy);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 50012160 AND rid <= 75018240) TO '/home/gienieczko/hdd/hits-strings-2-snappy.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION snappy);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 75018240) TO '/home/gienieczko/hdd/hits-strings-3-snappy.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION snappy);

COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid <= 25006080) TO '/home/gienieczko/hdd/hits-strings-0-zstd.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION zstd);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 25006080 AND rid <= 50012160) TO '/home/gienieczko/hdd/hits-strings-1-zstd.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION zstd);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 50012160 AND rid <= 75018240) TO '/home/gienieczko/hdd/hits-strings-2-zstd.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION zstd);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 75018240) TO '/home/gienieczko/hdd/hits-strings-3-zstd.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION zstd);

COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid <= 25006080) TO '/home/gienieczko/hdd/hits-strings-0-gzip.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION gzip);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 25006080 AND rid <= 50012160) TO '/home/gienieczko/hdd/hits-strings-1-gzip.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION gzip);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 50012160 AND rid <= 75018240) TO '/home/gienieczko/hdd/hits-strings-2-gzip.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION gzip);
COPY (SELECT rid, SearchPhrase, MobilePhoneModel, URL, Title, Referer FROM hits WHERE rid > 75018240) TO '/home/gienieczko/hdd/hits-strings-3-gzip.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE 122880, COMPRESSION gzip);
