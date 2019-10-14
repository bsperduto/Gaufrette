<?php

namespace Gaufrette\Functional\FileStream;

use Aws\S3\S3Client;
use Gaufrette\Filesystem;
use Gaufrette\Adapter\AwsS3;

class AwsS3Test extends FunctionalTestCase
{
    /** @var int */
    private static $SDK_VERSION;
    
    protected $directory;

    private $client;
    private $bucket;
    
    public function setUp()
    {
        $key = getenv('AWS_KEY');
        $secret = getenv('AWS_SECRET');
        
        if (empty($key) || empty($secret)) {
            $this->markTestSkipped('Either AWS_KEY and/or AWS_SECRET env variables are not defined.');
        }
        
        if (self::$SDK_VERSION === null) {
            self::$SDK_VERSION = method_exists(S3Client::class, 'getArguments') ? 3 : 2;
        }
        
        $this->bucket = uniqid(getenv('AWS_BUCKET'));
        
        if (self::$SDK_VERSION === 3) {
            // New way of instantiating S3Client for aws-sdk-php v3
            $this->client = new S3Client([
                'region' => 'eu-west-1',
                'version' => 'latest',
                'credentials' => [
                    'key' => $key,
                    'secret' => $secret,
                ],
            ]);
        } else {
            $this->client = S3Client::factory([
                'region' => 'eu-west-1',
                'version' => '2006-03-01',
                'key' => $key,
                'secret' => $secret,
            ]);
        }
        
        $this->createFilesystem(['create' => true]);
        //Force bucket creation
        $this->filesystem->write("empty.txt", "empty");
        sleep(5);
        $this->registerLocalFilesystemInStream();
    }

    private function createFilesystem(array $adapterOptions = [])
    {
        $this->filesystem = new Filesystem(new AwsS3($this->client, $this->bucket, $adapterOptions));
    }
    

    public function tearDown()
    {
        if ($this->client === null || !$this->client->doesBucketExist($this->bucket)) {
            return;
        }
        
        $result = $this->client->listObjects(['Bucket' => $this->bucket]);
        
        if (!$result->hasKey('Contents')) {
            $this->client->deleteBucket(['Bucket' => $this->bucket]);
            
            return;
        }
        
        foreach ($result->get('Contents') as $staleObject) {
            $this->client->deleteObject(['Bucket' => $this->bucket, 'Key' => $staleObject['Key']]);
        }
        
        $this->client->deleteBucket(['Bucket' => $this->bucket]);
    }
   
}
