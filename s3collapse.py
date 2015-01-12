import logging
import os
import tempfile
from datetime import datetime, timedelta
import boto

def collapse(s3bucket, inPrefix, outFile, outKey, outMaxSize=2*1024*1024*1024, outRRS = False):
    """
    concatenate all (small) files passed to the funtion into one larger one

    downloads keys (files) that match inPrefix, concatenates all of them 
    into outFile uploads outFile to outKey, then deletes downloaded keys

    Arguments:
    s3bucket
        s3bucket object. must already be initiated
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
        inKeys = s3bucket.list(inPrefix)
        if len(list(inKeys)) == 0:
            logging.info('  No files for this prefix')
            outFD.close()
            os.remove(outFile)
            return
        inSize = 0
        logging.info('  Downloading {:d} keys'.format(len(list(inKeys))))
        # True if the last file loaded was .gz, False otherwise
        wasGzip = None
        for inKey in inKeys:
            # keep track of total size to check at the end
            inSize = inSize + inKey.size 
            # create a temporary file that will reside in memory until it reaches
            # 16MB, at which point it gets written to disk. default is binary mode
            with tempfile.SpooledTemporaryFile(max_size = 16*1024*1024) as tempFD:
                inKey.get_contents_to_file(tempFD)
                # move back to start of file
                tempFD.flush()
                tempFD.seek(0)
                if wasGzip == None:
                    wasGzip = isGzip(tempFD, inKey.name)
                if isGzip(tempFD, inKey.name) != wasGzip:
                    # raise exception, we don't want to combine gzip and non-gzip files
                    if wasGzip:
                        fileType = 'not gzip'
                        filesType = 'gzip'
                    else:
                        fileType = 'gzip'
                        filesType = 'not gzip'
                    raise Exception('File {} is {}, but the other files so far were {}'.format(inKey.name, fileType, filesType))
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
    outKeyD = s3bucket.new_key(outKey)
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

def isGzip(fd, fn=None):
    '''
    determines if file is a gzip archive or not

    checks magic number (first two bytes) and file extension if passed a file
    name in order to determine if file is a gzip archive. returns True or False
    if a file name is also passed to the function, it return True if and only if
    file extension is '.gz' AND first two bytes are '1f 8b'. if only the magic
    number should be checked, don't give it a file name
    
    Arguments:
    fd (file descriptor)
        file descriptor. implies file is already open
    fn (str)
        file name
    '''
    
    fdPosition=fd.tell()
    fd.seek(0)
    byte1 = fd.read(1)
    byte2 = fd.read(1)
    fd.seek(fdPosition)
    # check magic number
    if byte1 == b'\x1f' and byte2 == b'\x8b':
        hasBytes = True
    else:
        hasBytes = False
    # check extention
    if type(fn) is str:
        if fn[-3:len(fn)] == '.gz':
            hasExtension = True
        else:
            hasExtension = False
    else:
        hasExtension = True

    return hasBytes and hasExtension

def collapse_s3(s3bucket, s3inDir, s3outDir=None, dateStart=None, dateEnd=None, increment="d", outRRS = False):
    '''
    mass collapse S3 logs generated in the specified time period

    start date and end date both default to yesterday unless specified
    so, if only dateStart is specified it will group logs from
    startDate to yesterday
    Ex.: collapse_s3(myBucket, 'logs/s3logs/', dateStart = datetime(2014,7,29))

    Arguments:
    s3bucket
        bucket object. must already be initiated
    s3inDir (str)
        s3 'path' where the logs reside
    dateStart (datetime)
        collapse logs starting this day...
    dateEnd (datetime)
        ...up to this day, inclusive. by default, yesterday 23:59:59
    increment (str)
        how to group logs. (d)aily/(H)ourly/(M)onthly
    s3outDir (str)
        directory where the resulting file will be stored on S3. 
        if unspecified it will be the same as s3inDir
    outRRS (boolean)
        use Reduced Redundancy Storage for the new file or not
        passed on to collapse()
    '''

    if increment == "d":
        timeStep = timedelta(days = 1)
    elif increment == "H":
        timeStep = timedelta(hours = 1)
    elif increment == "M":
        timeStep = timedelta(minutes = 1)
    else:
        raise RuntimeError('increment "' + increment + '" undefined, aborting')
    
    if dateEnd == None:
        # yesterday, one second before midnight
        dateEnd = datetime(datetime.today().year, datetime.today().month, datetime.today().day) - timedelta(seconds=1)
        if dateStart == None:
            # yesterday, midnight
            dateStart = datetime(dateEnd.year, dateEnd.month, dateEnd.day)

    if type(dateStart) != type(datetime.today()) or type(dateStart) != type(dateEnd) or dateEnd < dateStart:
        raise Exception("Start/End dates ({}/{}) are wrong somehow".format(str(dateStart), str(DateEnd)))

    s3inDir = os.path.join(s3inDir, '')
    if s3outDir == None:
        s3outDir = s3inDir
    else:
        s3outDir = os.path.join(s3outDir, '')
    outDir = os.path.join(tempfile.gettempdir(), '')

    dateCurrentLogs = dateStart
    while dateCurrentLogs <= dateEnd:
        prefix = dtm_to_s3_log(dateCurrentLogs, increment)
        inPrefix = s3inDir + prefix + "-"
        outFile = outDir + prefix + '_collapsed'
        outKey = s3outDir + prefix + '_collapsed'
        logging.info('{0}/{1}'.format(s3bucket.name, inPrefix))
        collapse(s3bucket, inPrefix, outFile, outKey, outRRS = outRRS)
        dateCurrentLogs = dateCurrentLogs + timeStep

def collapse_ctrail(s3bucket, region=None, account=None, s3outDir=None, dateStart=None, dateEnd=None, increment="d", outRRS = False):
    '''
    mass collapse CloudTrail logs generated in the specified time period

    start date and end date both default to yesterday unless specified
    if only dateStart is specified it will group logs from
    startDate to yesterday
    Ex.: collapse_ctrail(myBucket, 'us-east-1', '12345678900', datestart = datetime(2014,7,29), outRRS = True)

    Arguments:
    s3bucket
        bucket object. must already be initiated
    s3outDir (str)
        directory where the resulting file will be stored on S3. 
        if unspecified it will be the same as s3inDir
        takes precedent over region/account
    region (str)
        if specified it will be used along with account to construct
        s3dirIn and s3outDir
    dateStart (datetime)
        collapse logs starting this day...
    dateEnd (datetime)
        ...up to this day, inclusive. by default, yesterday 23:59:59
    increment (str)
        how to group logs. (d)aily/(H)ourly/(M)onthly
    outRRS (boolean)
        use Reduced Redundancy Storage for the new file or not
        passed on to collapse()
    '''


    if dateEnd == None:
        # yesterday, one second before midnight
        dateEnd = datetime(datetime.today().year, datetime.today().month, datetime.today().day) - timedelta(seconds=1)
        if dateStart == None:
            # yesterday, midnight
            dateStart = datetime(dateEnd.year, dateEnd.month, dateEnd.day)

    dt = dateStart
    while dt <= dateEnd:
        if type(region) is str and type(account) is str: 
            s3inDir = 'AWSLogs/{}/CloudTrail/{}/{:%Y/%m/%d}/'.format(account, region, dt)
        else:
            raise Exception("Region and/or account not specified")
        s3inFile = '{}_CloudTrail_{}_{:%Y%m%d}T'.format(account, region, dt)
        s3inPrefix = s3inDir + s3inFile
        if type(s3outDir) is str:
            # defined, all good
            pass # Python doesn't like empty blocks
        elif type(region) is str and type(account) is str:
            s3outDir = 'AWSLogs_collapsed/{}/CloudTrail/{}/'.format(account, region)
        else:
            raise Exception("s3outDir could not be defined")
        outFile = '{}_CloudTrail_{}_{:%Y%m%d}_collapsed'.format(account, region, dt)
        s3outFile = s3outDir + outFile
        logging.info('{}: {} -> {}'.format(s3bucket.name, s3inPrefix, s3outFile))
        collapse(s3bucket, s3inPrefix, outFile, s3outFile, outRRS = outRRS)
        dt = dt + timedelta(days=1)
