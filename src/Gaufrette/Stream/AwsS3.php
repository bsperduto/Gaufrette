<?php

namespace Gaufrette\Stream;

use Gaufrette\Stream;
use Gaufrette\StreamMode;
use Aws\S3\S3Client;
use Aws\S3\StreamWrapper;

/**
 * AwsS3 stream.
 *
 */
class AwsS3 implements Stream
{
    private $key;
    private $mode;
    private $fileHandle;
    private $mkdirMode;
    /**
     * @var S3Client
     */
    private $client;
    /**
     * @var String
     */
    private $bucket;
    private $seekable = false;
    private $tempStream = false;
    private $detectContentType;

    /**
     * @param S3Client $client
     * @param String   $bucket
     */
    public function __construct($client, $bucket, $key, $detectContentType)
    {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->key = $key;
        $this->detectContentType = $detectContentType;
    }

    /**
     * {@inheritdoc}
     */
    public function open(StreamMode $mode)
    {
        if ($mode->allowsRead() && $mode->allowsWrite()) {
            throw new \RuntimeException("AwsS3 SDK does not support reading and writing from the same stream");
        }
        $this->checkAndRegisterWrapper();
        if($this->detectContentType && $mode->impliesExistingContentDeletion()) {
            $this->fileHandle = fopen("php://temp", "w+");
            $this->tempStream = true;
        }
        else {
            $fileHandle = $this->openStream($mode);
        }

        if (false === $fileHandle) {
            throw new \RuntimeException(sprintf('File "%s" cannot be opened', $this->key));
        }

        $this->mode = $mode;
        $this->fileHandle = $fileHandle;

        return true;
    }
    

    /**
     * {@inheritdoc}
     */
    public function read($count)
    {
        if (!$this->fileHandle) {
            return false;
        }

        if (false === $this->mode->allowsRead()) {
            throw new \LogicException('The stream does not allow read.');
        }

        return fread($this->fileHandle, $count);
    }

    /**
     * {@inheritdoc}
     */
    public function write($data)
    {
        if (!$this->fileHandle) {
            return false;
        }

        if (false === $this->mode->allowsWrite()) {
            throw new \LogicException('The stream does not allow write.');
        }

        return fwrite($this->fileHandle, $data);
    }

    /**
     * {@inheritdoc}
     */
    public function close()
    {
        if (!$this->fileHandle) {
            return false;
        }
        
        if($this->tempStream) {
            rewind($this->fileHandle);
            $fileInfo = new \finfo(FILEINFO_MIME_TYPE);
            $mime =  $fileInfo->buffer(fread($this->fileHandle, 1048576));
            rewind($this->fileHandle);
            $context = stream_context_create([
                's3' => ['ContentType' => $mime]
            ]);
            $writeHandle = $this->openStream($this->mode, $context);
            stream_copy_to_stream($this->fileHandle, $writeHandle);
            fclose($this->fileHandle);
            $this->fileHandle = $writeHandle;
        }
        //S3 requires advance flush
        fflush($this->fileHandle);
        $closed = fclose($this->fileHandle);

        if ($closed) {
            $this->mode = null;
            $this->fileHandle = null;
        }

        return $closed;
    }

    /**
     * {@inheritdoc}
     */
    public function flush()
    {
        if ($this->fileHandle) {
            return fflush($this->fileHandle);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function seek($offset, $whence = SEEK_SET)
    {
        if ($this->fileHandle) {
            if (!$this->seekable) {
                fclose($this->fileHandle);
                $context = stream_context_create([
                    's3' => ['seekable' => true]
                ]);
                $this->fileHandle = $this->openStream($this->mode, $context);
                $this->seekable = true;
            }
            return 0 === fseek($this->fileHandle, $offset, $whence);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function tell()
    {
        if ($this->fileHandle) {
            return ftell($this->fileHandle);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function eof()
    {
        if ($this->fileHandle) {
            return feof($this->fileHandle);
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function stat()
    {
        if ($this->fileHandle) {
            return fstat($this->fileHandle);
        } elseif (!is_resource($this->fileHandle) && is_dir($this->path)) {
            return stat($this->path);
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function cast($castAs)
    {
        if ($this->fileHandle) {
            return $this->fileHandle;
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function unlink()
    {
        if ($this->mode && $this->mode->impliesExistingContentDeletion()) {
            return @unlink($this->path);
        }

        return false;
    }
    
    private function checkAndRegisterWrapper()
    {
        if (!in_array("s3-gaufrette",stream_get_wrappers())) {
            StreamWrapper::register($this->client, "s3-gaufrette");
        }
    }
    
    private function openStream($mode, $context=null) {
        $path = "s3-gaufrette://".$this->bucket."/".$this->key;
        if($context !== null) {
            try {
                $fileHandle = @fopen($path, $mode->getMode(), false, $context);
            } catch (\Exception $e) {
                $fileHandle = false;
            }
        }
        else {
            try {
                $fileHandle = @fopen($path, $mode->getMode());
            } catch (\Exception $e) {
                $fileHandle = false;
            }
        }
        return $fileHandle;
    }
}
