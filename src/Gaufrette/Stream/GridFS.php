<?php
namespace Gaufrette\Stream;

use Gaufrette\Stream;
use Gaufrette\StreamMode;

class GridFS extends InMemoryBuffer implements Stream
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
        return parent::__construct($filesystem, $key);
    }

    public function open(StreamMode $mode)
    {
        $this->mode = $mode;

        if ($this->mode->allowsRead() && $this->mode->allowsWrite()) {
            $this->gridfsstream = false;
            // GridFS only supports reading or writing not both so revert to in memory
            return parent::open($mode);
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
        if (!$this->gridfsstream) {
            return parent::read($count);
        }
        
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
        if (!$this->gridfsstream) {
            return parent::write($data);
        }
        
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
        if (!$this->gridfsstream) {
            return parent::close();
        }
        
        if (! $this->handle) {
            return false;
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
            return parent::flush();
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
        if (!$this->gridfsstream) {
            return parent::seek($offset, $whence);
        }
        
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
        if (!$this->gridfsstream) {
            return parent::tell();
        }
        
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
        if (!$this->gridfsstream) {
            return parent::eof();
        }
        
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
        if (!$this->gridfsstream) {
            return parent::stat();
        }
        
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
        if (!$this->gridfsstream) {
            return parent::cast($castAs);
        }
        
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
        if (!$this->gridfsstream) {
            return parent::unlink();
        }
        
        if ($this->mode && $this->mode->impliesExistingContentDeletion()) {
            return $this->filesystem->delete($this->key);
        }
        return false;
    }
}

