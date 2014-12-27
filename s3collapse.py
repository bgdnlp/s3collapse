import logging
import os
import tempfile
from datetime import datetime, timedelta
import boto

def collapse(bucket, inPrefix, outFile, outKey, outMaxSize=2*1024*1024*1024, outRRS = False):
    """
    concatenate all (small) files passed to the funtion into one larger one

    downloads keys (files) that match inPrefix, concatenates all of them 
    into outFile uploads outFile to outKey, then deletes downloaded keys

    Arguments:
    bucket (str)
        bucket object. must already be initiated
    inPrefix (str)
        prefix used to list keys, like wildcard (i.e. path/to/files*)
    outFile (str)
        local file name that the others will be collapsed into
    outKey (str)
        S3 key where the contents of outFile will be written to (i.e. uploaded file)
    outMaxSize (int)
        if the outFile size grows over this value, raise exception and cancel.
        defaults to 2GB, set to 0 to disable
    outRRS (str)
        if True the new key will be set to use Reduced Redundancy Storage
    """
    # DOWNLOADING AND CONCATENATING
    # open outFile in binary mode because get_contents_* returns bytes
    with open(outFile, mode='wb') as outFD:
        inKeys = bucket.list(inPrefix)
        if len(list(inKeys)) == 0:
            logging.info('  No files for this prefix')
            return
        inSize = 0
        logging.info('  Downloading {:d} keys'.format(len(list(inKeys))))
        for inKey in inKeys:
            # keep track of total size to check at the end
            inSize = inSize + inKey.size 
            # create a temporary file that will reside in memory until it reaches
            # 16MB, at which point it gets written to disk. default is binary mode
            #logging.info('  Downloading {}'.format(inKey))
            with tempfile.SpooledTemporaryFile(max_size = 16*1024*1024) as tempFD:
                inKey.get_contents_to_file(tempFD)
                # move back to start of file
                tempFD.flush()
                tempFD.seek(0)
                # read tempfile 256KB at a time, write to outFile
                inChunk = tempFD.read(256*1024)
                while len(inChunk):
                    outFD.write(inChunk)
                    inChunk = tempFD.read(256*1024)
            if  outMaxSize > 0 and outFD.tell() > outMaxSize:
                os.remove(outFile)
                raise RuntimeError("Output file size bigger than the maximum of " + str(outMaxSize) + "B. Removed " \
                        + outFile + ", canceling operation")
                outFD.close()
        # any problems should have already interrupted execution, 
        # but this double-check is cheap
        outSize = outFD.tell()
        if inSize != outSize:
            raise RuntimeError("Collapsed file size of " + str(outSize) + " bytes is different than the expected " \
                    + str(inSize) + " bytes!")
    # UPLOADING CONCATENATED FILE
    # files downloaded and concatenated, uploading outFile
    # could maybe go with multipart uploads here
    logging.info('  Uploading {} ({:d} Bytes) to {}'.format(outFile, outSize, outKey))
    outKeyD = bucket.new_key(outKey)
    outKeySize = outKeyD.set_contents_from_filename(outFile, replace=False, reduced_redundancy=outRRS, cb=s3_progress, num_cb=3)
    if outKeySize == None:
        outKeySize = 0
    if outKeySize != outSize:
        raise RuntimeError("Uploaded file size (" + str(outKeySize) + "B) differs from local file size (" + str(outSize) + "B)")
    # DELETE COLLAPSED KEYS
    logging.info('  Removing {:d} keys'.format(len(list(inKeys))))
    for inKey in inKeys:
        inKey.delete()
    # DELETE LOCAL CONCATENATED FILE
    os.remove(outFile)


def s3_progress(current, total):
    '''
    called by s3 upload/download functions to display progress
    '''

    logging.info('    Transferred {:d}%'.format(int(current/total*100)))


def dtm_to_s3_log(dtm, increment):
    '''
    convert datetime to a string like 2014-12-31

    will convert dtm to the appropriate prefix string, depending
    on the selected incrment. For S3 logs, which have names like 
    2014-12-31-17-25-36-XXXXXXXXXXXXXXXX

    Arguments:
    dtm (datetime)
        object to be converted
    increment (str)
        what kind of stepping will be used
    '''
    # {:%Y-%m-%d-%H-%M-%S-}
    if increment == "d": # day
        dtstr = '{:%Y-%m-%d}'.format(dtm)
    elif increment == "H": # hour
        dtstr = '{:%Y-%m-%d-%H}'.format(dtm)
    elif increment == "M": # minute
        dtstr = '{:%Y-%m-%d-%H-%M}'.format(dtm)
    elif increment == "S": # second
        dtstr = '{:%Y-%m-%d-%H-%M-%S}'.format(dtm)
    elif increment == "m": # month
        dtstr = '{:%Y-%m}'.format(dtm)
    elif increment == "Y": # year
        dtstr = '{:%Y}'.format(dtm)
    else:
        raise RuntimeError('increment "' + increment + '" undefined, aborting')

    return dtstr

def collapse_s3_backlog(s3BucketName, s3dir, dateStart, \
        dateEnd=datetime(datetime.today().year,datetime.today().month,datetime.today().day)-timedelta(seconds=1), \
        increment="d"):
    '''
    mass collapse S3 logs generated in the specified time period 

    Arguments:
    s3BucketName (str)
        name of the bucket
    s3dir (str)
        s3 'path' where the logs reside
    dateStart (datetime)
        collapse logs starting this day...
    dateEnd (datetime)
        ...up to this day, inclusive. by default, yesterday 23:59:59
    increment (str)
        how to group logs. (d)aily/(H)ourly/(M)onthly 

    Ex.: collapse_s3_backlog('myBucket', 'logs/s3logs/', datetime(2014,7,29))
    '''

    if increment == "d":
        timeStep = timedelta(days = 1)
    elif increment == "H":
        timeStep = timedelta(hours = 1)
    elif increment == "M":
        timeStep = timedelta(minutes = 1)
    else:
        raise RuntimeError('increment "' + increment + '" undefined, aborting')
    
    s3conn = boto.connect_s3()
    s3bucket = s3conn.get_bucket(s3BucketName)

    s3dir = os.path.join(s3dir, '')
    outDir = os.path.join(tempfile.gettempdir(), '')

    dateCurrentLogs = dateStart
    while dateCurrentLogs <= dateEnd:
        prefix = dtm_to_s3_log(dateCurrentLogs, increment)
        inPrefix = s3dir + prefix + "-"
        outFile = outDir + prefix + '_collapsed'
        outKey = s3dir + prefix + '_collapsed'
        logging.info('{0}/{1}'.format(s3BucketName, inPrefix))
        collapse(s3bucket, inPrefix, outFile, outKey)
        dateCurrentLogs = dateCurrentLogs + timeStep

