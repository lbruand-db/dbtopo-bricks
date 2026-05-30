TODO
====

 * [ ] Licensing (?)
 * [ ] Add with outremer
 * [ ] Download adresses. Build a geocoder
 * [ ] Make a blog post
 * [ ] extract the logic around downloading / unpacking / reading the files into the read() method 
       of a pyspark.sql.datasource.DataSourceReader object then register a custom datasource and 
       use e.g. spark.read.format("bdtopo").load() to pull in the data? You could spawn a partition per department
       to parallelise the process.
 