<?php
namespace Gaufrette\Stream;

use Gaufrette\Stream;
use Gaufrette\StreamMode;

class GridFS implements Stream
{

    private $gridfsstream;
    private $key;
    private $handle;
    private $filesystem;
    private $mode;

    public function __construct($filesystem, $key)
    {
        $this->filesystem = $filesystem;
        $this->key = $key;
    }

    public function open(StreamMode $mode)
    {
        $this->mode = $mode;

        if ($this->mode->allowsRead() && $this->mode->allowsWrite()) {
            $this->gridfsstream = false;
            // GridFS only supports reading or writing not both so revert to in memory
            $this->handle = fopen("php://temp", $this->mode->getMode());
            
            if (! $this->mode->impliesExistingContentDeletion() && $this->filesystem->has($this->key)) {
                try {
                    $readhandle = $this->filesystem->getAdapter()
                    ->getBucket()
                    ->openDownloadStreamByName($this->key);
                } catch (\Exception $e) {
                    throw new \RuntimeException(sprintf('File "%s" cannot be opened', $this->key));
                }
                stream_copy_to_stream($readhandle, $this->handle);
                fclose($readhandle);
            }
            if ($this->mode->impliesPositioningCursorAtTheEnd()) {
                fseek($this->handle, 0, SEEK_END);
            } else {
                fseek($this->handle, 0, SEEK_SET);
            }
            return true;
        }
        $this->gridfsstream = true;
        $exists = $this->filesystem->has($this->key);
        if (($exists && ! $mode->allowsExistingFileOpening()) || (! $exists && ! $mode->allowsNewFileOpening())) {
            return false;
        }

        if ($this->mode->allowsRead()) {
            try {
                $this->handle = $this->filesystem->getAdapter()
                    ->getBucket()
                    ->openDownloadStreamByName($this->key);
            } catch (\Exception $e) {
                throw new \RuntimeException(sprintf('File "%s" cannot be opened', $this->key));
            }
            if ($this->mode->impliesPositioningCursorAtTheEnd()) {
                fseek($this->handle, 0, SEEK_END);
            }

            return true;
        } else {
            if ($this->mode->impliesExistingContentDeletion()) {
                if ($this->filesystem->has($this->key)) {
                    $this->filesystem->delete($this->key);
                }
            }

            try {
                $this->handle = $this->filesystem->getAdapter()
                    ->getBucket()
                    ->openUploadStream($this->key);
            } catch (\Exception $e) {
                throw new \RuntimeException(sprintf('File "%s" cannot be opened', $this->key));
            }

            if (! $this->mode->impliesExistingContentDeletion() && $this->filesystem->has($this->key)) {
                try {
                    $readhandle = $this->filesystem->getAdapter()
                        ->getBucket()
                        ->openDownloadStreamByName($this->key);
                } catch (\Exception $e) {
                    throw new \RuntimeException(sprintf('File "%s" cannot be opened', $this->key));
                }
                stream_copy_to_stream($readhandle, $this->handle);
                fclose($readhandle);
            }

            if ($this->mode->impliesPositioningCursorAtTheEnd()) {
                fseek($this->handle, 0, SEEK_END);
            } else {
                fseek($this->handle, 0, SEEK_SET);
            }

            return true;
        }

        return false;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function read($count)
    {
        if (! $this->handle) {
            return false;
        }
        if (false === $this->mode->allowsRead()) {
            throw new \LogicException('The stream does not allow read.');
        }
        
        return fread($this->handle, $count);
    }

    /**
     *
     * {@inheritdoc}
     */
    public function write($data)
    {
        if (! $this->handle) {
            return false;
        }
        if (false === $this->mode->allowsWrite()) {
            throw new \LogicException('The stream does not allow write.');
        }
        return fwrite($this->handle, $data);
    }

    /**
     *
     * {@inheritdoc}
     */
    public function close()
    {
        if (! $this->handle) {
            return false;
        }
        
        if (!$this->gridfsstream) {
            //Flush to GridFS first
            $this->flush();
        }
        
        
        $closed = fclose($this->handle);
        if ($closed) {
            $this->mode = null;
            $this->handle = null;
        }
        return $closed;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function flush()
    {
        if (!$this->gridfsstream) {
            if ($this->filesystem->has($this->key)) {
                $this->filesystem->delete($this->key);
            }
            try {
                $writehandle = $this->filesystem->getAdapter()
                ->getBucket()
                ->openUploadStream($this->key);
                stream_copy_to_stream($this->handle, $writehandle, -1, 0);
                fclose($writehandle);
                return true;
            } catch (\Exception $e) {
                throw new \RuntimeException(sprintf('File "%s" cannot be opened', $this->key));
            }
        }
        
        if ($this->handle) {
            return fflush($this->handle);
        }
        return false;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function seek($offset, $whence = SEEK_SET)
    {
        
        if ($this->handle) {
            return 0 === fseek($this->handle, $offset, $whence);
        }
        return false;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function tell()
    {
        
        if ($this->handle) {
            return ftell($this->handle);
        }
        return false;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function eof()
    {
        
        if ($this->handle) {
            return feof($this->handle);
        }
        return true;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function stat()
    {
        
        if ($this->handle) {
            return fstat($this->handle);
        }
        return false;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function cast($castAs)
    {
        
        if ($this->handle) {
            return $this->handle;
        }
        return false;
    }

    /**
     *
     * {@inheritdoc}
     */
    public function unlink()
    {        
        if ($this->mode && $this->mode->impliesExistingContentDeletion()) {
            return $this->filesystem->delete($this->key);
        }
        return false;
    }
}

