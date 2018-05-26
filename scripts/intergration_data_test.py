import copy
# only for intergrating test
def gen_aws_test_data():
    fake = {
            'fileid': '1',
            'filename': 'tmp1G.img',
            'size': 1,
            'hash': 'd13dfabb799670aaf1fd00e1ab55907e',
            'project': 'TGCA',
            'acl': '*'}
    fake2 = {
            'fileid': '2',
            'filename': 'tmp1G.img',
            'size': 1,
            'hash': 'd13dfabb799670aaf1fd00e1ab55907e',
            'project': 'TGCA',
            'acl': 'tgca'}

    fake3 = {
            'fileid': '3',
            'filename': 'tmp1G.img',
            'size': 1,
            'hash': 'd13dfabb799670aaf1fd00e1ab55907e',
            'project': 'TGCA',
            'acl': 'tgca'}

    l = [fake, fake2, fake3]
    for i in xrange(4, 11):
        tmp = copy.deepcopy(l[i % 3])
        tmp['filename'] = 'abc{}.bam'.format(i)
        l.append(tmp)
    return l


def google_gen_test_data():
    fake = {
             'fileid': '08fed839-3abd-4367-a540-5d7888de38d6',
	     'filename': 'TCGA-FF-8046-01A-11D-2210-10_Illumina_gdc_realn.bam',
             'size': 1,
             'hash': '90ebae47ecf0d4131939c3354cb9d9e9',
             'project': 'TGCA',
             'acl': '*'}
    fake2 = {
             'fileid': '0b7fb96a-9d4d-4839-9c89-71b22c0dbe24',
             'filename': 'TCGA-FA-A4XK-01A-11D-A31X-10_Illumina_gdc_realn.bam',
             'size': 1,
             'hash': 'cdbac140adc13d3e6cce0b6fdb69d210',
             #'hash': '90ebae47ecf0d4131939c3354cb9d9e9',
             'project': 'TGCA',
             'acl': 'tgca'}

    l = [fake, fake2]
    #for i in xrange(3, 5):
     #   tmp = copy.deepcopy(l[i % 2])
    #    tmp['filename'] = 'abc{}.bam'.format(i)
    #    l.append(tmp)
    return l
