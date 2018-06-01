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
             'fileid': '55170f39-e23c-4c70-8658-2aeb9b951b1c',
             'filename': 'TCGA-DX-A3LY-01B-01-TSA.2BC7977C-69D9-4C53-BCA2-9D7D266A28BB.svs',
             'size': 1,
             'hash': '342fe2cece840fb158915df41a49aa5f',
             'project': 'TGCA',
             'acl': '*'}
    fake2 = {
             'fileid': '7f3f4cfc-6e8e-418d-a278-b74afef644d5',
             'filename': 'TCGA-PK-A5HA-01A-01-TS1.B381126C-4CA5-4DF9-A8E2-C0CA0207F365.svs',
             'size': 1,
             'hash': '362f2974ec2fb70f0225b5a853c126f8',
             #'hash': '90ebae47ecf0d4131939c3354cb9d9e9',
             'project': 'TGCA',
             'acl': 'tgca'} #fake controled

    l = [fake, fake2]
    for i in xrange(3, 10):
        tmp = copy.deepcopy(l[i % 2])
        tmp['filename'] = 'abc{}.bam'.format(i)
        l.append(tmp)
    return l

