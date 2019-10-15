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
    
    /**
     * @test
     */
    public function shouldWriteFromSettedPosition()
    {
        $this->markTestSkipped('Unsupported.');
    }
    
    /**
     * @test
     */
    public function shouldHandlesSubDir()
    {
        file_put_contents('gaufrette://filestream/subdir/test.txt', 'test content');
        
        $this->assertTrue(is_file('gaufrette://filestream/subdir/test.txt'));
        
        $this->filesystem->delete('subdir/test.txt');
        clearstatcache();
        $this->assertFalse(is_file('gaufrette://filestream/subdir/test.txt'));
    }
    
    /**
     * @test
     */
    public function shouldSetAndGetPosition()
    {
        file_put_contents('gaufrette://filestream/test.txt', 'test content');
        
        $fileHandler = fopen('gaufrette://filestream/test.txt', 'r');
        fseek($fileHandler, 1, SEEK_SET);
        $this->assertEquals(1, ftell($fileHandler));
        fseek($fileHandler, 1, SEEK_CUR);
        $this->assertEquals(2, ftell($fileHandler));
        fclose($fileHandler);
        
        $fileHandler = fopen('gaufrette://filestream/test.txt', 'r');
        fseek($fileHandler, 1, SEEK_CUR);
        $this->assertEquals(1, ftell($fileHandler));
        fclose($fileHandler);
        
        $fileHandler = fopen('gaufrette://filestream/test.txt', 'r');
        fseek($fileHandler, -2, SEEK_END);
        $this->assertEquals(10, ftell($fileHandler));
        fclose($fileHandler);
    }
    
    /**
     * @test
     */
    public function shouldNotSeekWhenWhenceParameterIsInvalid()
    {
        file_put_contents('gaufrette://filestream/test.txt', 'test content');
        
        $fileHandler = fopen('gaufrette://filestream/test.txt', 'r');
        $this->assertEquals(-1, fseek($fileHandler, 1, 666));
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
    
    public static function modesProvider()
    {
        return [
            ['w'],
            ['a'],
            ['ab'],
            ['wb'],
        ];
    }
   
}
