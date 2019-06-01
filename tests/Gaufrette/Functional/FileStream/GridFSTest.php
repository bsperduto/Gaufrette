<?php

namespace Gaufrette\Functional\FileStream;

use Gaufrette\Filesystem;
use Gaufrette\Adapter\GridFS;
use MongoDB\Client;

class GridFSTest extends FunctionalTestCase
{
    public function setUp()
    {
        $uri = getenv('MONGO_URI');
        $dbname = getenv('MONGO_DBNAME');
        
        if ($uri === false || $dbname === false) {
            $this->markTestSkipped('Either MONGO_URI or MONGO_DBNAME env variables are not defined.');
        }
        
        $client = new Client($uri);
        $db = $client->selectDatabase($dbname);
        $bucket = $db->selectGridFSBucket();
        $bucket->drop();
        
        $this->filesystem = new Filesystem(new GridFS($bucket));

        $this->registerLocalFilesystemInStream();
    }
}
