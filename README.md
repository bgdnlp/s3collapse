s3collapse
==========
Mostly a learning project, the script provides functions to download (small) files, concatenate them into one, upload the new file, then delete the small ones. Intended for collapsing many S3/AWS small logs into a daily/hourly larger one.

AWS logs stored on S3 are generated every few minutes in the case of CloudTrail or every few seconds in the case of S3. These can be very small and there can be a lot of them. Annoying to download and more objects imply higher costs in metadata storage and number of requests.
