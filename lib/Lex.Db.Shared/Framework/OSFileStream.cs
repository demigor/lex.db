#if NETFX_CORE
#if !SILVERLIGHT && !WINDOWS_PHONE
using System;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace Lex.Db
{
#if NETFX_CORE
  [Flags]
  enum FileAccess : uint
  {
    /// <summary>
    /// Read access.
    /// </summary>
    Read = 0x80000000,

    /// <summary>
    /// Write access.
    /// </summary>
    Write = 0x40000000,

    /// <summary>
    /// Read/Write Access,
    /// </summary>
    ReadWrite = Read | Write,

    /// <summary>
    /// Execute access.
    /// </summary>
    Execute = 0x20000000,

    /// <summary>
    /// All access
    /// </summary>
    All = 0x10000000
  }

  /// <summary>Contains constants for controlling the kind of access other <see cref="T:System.IO.FileStream" /> objects can have to the same file.</summary>
  [Flags]
  enum FileShare
  {
    /// <summary>Declines sharing of the current file. Any request to open the file (by this process or another process) will fail until the file is closed.</summary>
    None = 0,
    /// <summary>Allows subsequent opening of the file for reading. If this flag is not specified, any request to open the file for reading (by this process or another process) will fail until the file is closed. However, even if this flag is specified, additional permissions might still be needed to access the file.</summary>
    Read = 1,
    /// <summary>Allows subsequent opening of the file for writing. If this flag is not specified, any request to open the file for writing (by this process or another process) will fail until the file is closed. However, even if this flag is specified, additional permissions might still be needed to access the file.</summary>
    Write = 2,
    /// <summary>Allows subsequent opening of the file for reading or writing. If this flag is not specified, any request to open the file for reading or writing (by this process or another process) will fail until the file is closed. However, even if this flag is specified, additional permissions might still be needed to access the file.</summary>
    ReadWrite = 3,
    /// <summary>Allows subsequent deleting of a file.</summary>
    Delete = 4
  }

  /// <summary>Specifies how the operating system should open a file.</summary>
  enum FileMode : uint
  {
    /// <summary>Specifies that the operating system should create a new file. This operation requires <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Write" /> permission. If the file already exists, an <see cref="T:System.IO.IOException" /> exception is thrown.</summary>
    CreateNew = 1,
    /// <summary>Specifies that the operating system should create a new file. If the file already exists, it will be overwritten. This operation requires <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Write" /> permission. System.IO.FileMode.Create is equivalent to requesting that if the file does not exist, use <see cref="F:System.IO.FileMode.CreateNew" />; otherwise, use <see cref="F:System.IO.FileMode.Truncate" />. If the file already exists but is a hidden file, an <see cref="T:System.UnauthorizedAccessException" /> exception is thrown.</summary>
    Create = 2,
    /// <summary>Specifies that the operating system should open an existing file. The ability to open the file is dependent on the value specified by the <see cref="T:System.IO.FileAccess" /> enumeration. A <see cref="T:System.IO.FileNotFoundException" /> exception is thrown if the file does not exist.</summary>
    Open = 3,
    /// <summary>Specifies that the operating system should open a file if it exists; otherwise, a new file should be created. If the file is opened with FileAccess.Read, <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Read" /> permission is required. If the file access is FileAccess.Write, <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Write" /> permission is required. If the file is opened with FileAccess.ReadWrite, both <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Read" /> and <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Write" /> permissions are required.  </summary>
    OpenOrCreate = 4,
    /// <summary>Specifies that the operating system should open an existing file. When the file is opened, it should be truncated so that its size is zero bytes. This operation requires <see cref="F:System.Security.Permissions.FileIOPermissionAccess.Write" /> permission. Attempts to read from a file opened with FileMode.Truncate cause an exception.</summary>
    Truncate = 5
  }

  [Flags]
  enum FileAttributes : int
  {
    Invalid = -1,
    Directory = 0x10,
  }

#else
  [Flags]
  enum FileOptions : uint
  {
    /// <summary>
    /// None attribute.
    /// </summary>
    None = 0x00000000,

    /// <summary>
    /// Read only attribute.
    /// </summary>
    ReadOnly = 0x00000001,

    /// <summary>
    /// Hidden attribute.
    /// </summary>
    Hidden = 0x00000002,

    /// <summary>
    /// System attribute.
    /// </summary>
    System = 0x00000004,

    /// <summary>
    /// Directory attribute.
    /// </summary>
    Directory = 0x00000010,

    /// <summary>
    /// Archive attribute.
    /// </summary>
    Archive = 0x00000020,

    /// <summary>
    /// Device attribute.
    /// </summary>
    Device = 0x00000040,

    /// <summary>
    /// Normal attribute.
    /// </summary>
    Normal = 0x00000080,

    /// <summary>
    /// Temporary attribute.
    /// </summary>
    Temporary = 0x00000100,

    /// <summary>
    /// Sparse file attribute.
    /// </summary>
    SparseFile = 0x00000200,

    /// <summary>
    /// ReparsePoint attribute.
    /// </summary>
    ReparsePoint = 0x00000400,

    /// <summary>
    /// Compressed attribute.
    /// </summary>
    Compressed = 0x00000800,

    /// <summary>
    /// Offline attribute.
    /// </summary>
    Offline = 0x00001000,

    /// <summary>
    /// Not content indexed attribute.
    /// </summary>
    NotContentIndexed = 0x00002000,

    /// <summary>
    /// Encrypted attribute.
    /// </summary>
    Encrypted = 0x00004000,

    /// <summary>
    /// Write through attribute.
    /// </summary>
    Write_Through = 0x80000000,

    /// <summary>
    /// Overlapped attribute.
    /// </summary>
    Overlapped = 0x40000000,

    /// <summary>
    /// No buffering attribute.
    /// </summary>
    NoBuffering = 0x20000000,

    /// <summary>
    /// Random access attribute.
    /// </summary>
    RandomAccess = 0x10000000,

    /// <summary>
    /// Sequential scan attribute.
    /// </summary>
    SequentialScan = 0x08000000,

    /// <summary>
    /// Delete on close attribute.
    /// </summary>
    DeleteOnClose = 0x04000000,

    /// <summary>
    /// Backup semantics attribute.
    /// </summary>
    BackupSemantics = 0x02000000,

    /// <summary>
    /// Post semantics attribute.
    /// </summary>
    PosixSemantics = 0x01000000,

    /// <summary>
    /// Open reparse point attribute.
    /// </summary>
    OpenReparsePoint = 0x00200000,

    /// <summary>
    /// Open no recall attribute.
    /// </summary>
    OpenNoRecall = 0x00100000,

    /// <summary>
    /// First pipe instance attribute.
    /// </summary>
    FirstPipeInstance = 0x00080000
  }

#endif

  class OSFileStream : Stream
  {
    IntPtr _handle;
    bool _canRead, _canWrite, _canSeek;
    long _position;

    public OSFileStream(string fileName, FileMode fileMode, FileAccess access, FileShare share = FileShare.Read)
    {
#if NETFX_CORE
      _handle = WinApi.CreateFile(fileName, access, share, fileMode, IntPtr.Zero);
#else
      _handle = WinApi.CreateFile(fileName, access, share, IntPtr.Zero, fileMode, FileOptions.None, IntPtr.Zero);
#endif
      if (_handle == new IntPtr(-1))
        throw new IOException(string.Format(CultureInfo.InvariantCulture, "Unable to open file {0}", fileName), Marshal.GetLastWin32Error());

      _canRead = 0 != (access & FileAccess.Read);
      _canWrite = 0 != (access & FileAccess.Write);
      _canSeek = true;
    }

    public override void Flush()
    {
      if (!WinApi.FlushFileBuffers(_handle))
        throw new IOException("Unable to flush stream", Marshal.GetLastWin32Error());
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
      long newPosition;

      if (!WinApi.SetFilePointerEx(_handle, offset, out newPosition, origin))
        throw new IOException("Unable to seek to this position", Marshal.GetLastWin32Error());

      _position = newPosition;
      return _position;
    }

    public override void SetLength(long value)
    {
      long newPosition;

      if (!WinApi.SetFilePointerEx(_handle, value, out newPosition, SeekOrigin.Begin))
        throw new IOException("Unable to seek to this position", Marshal.GetLastWin32Error());

      if (!WinApi.SetEndOfFile(_handle))
        throw new IOException("Unable to set the new length", Marshal.GetLastWin32Error());

      if (_position < value)
      {
        Seek(_position, SeekOrigin.Begin);
      }
      else
      {
        Seek(0, SeekOrigin.End);
      }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
      if (buffer == null)
        throw new ArgumentNullException("buffer");

      unsafe
      {
        fixed (void* pbuffer = buffer)
          return Read((IntPtr)pbuffer, offset, count);
      }
    }

    public int Read(IntPtr buffer, int offset, int count)
    {
      if (buffer == IntPtr.Zero)
        throw new ArgumentNullException("buffer");

      int numberOfBytesRead;
      unsafe
      {
        void* pbuffer = (byte*)buffer + offset;
        {
          if (!WinApi.ReadFile(_handle, (IntPtr)pbuffer, count, out numberOfBytesRead, IntPtr.Zero))
            throw new IOException("Unable to read from file", Marshal.GetLastWin32Error());
        }
        _position += numberOfBytesRead;
      }
      return numberOfBytesRead;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
      if (buffer == null)
        throw new ArgumentNullException("buffer");

      unsafe
      {
        fixed (void* pbuffer = buffer)
          Write((IntPtr)pbuffer, offset, count);
      }
    }

    public void Write(IntPtr buffer, int offset, int count)
    {
      if (buffer == IntPtr.Zero)
        throw new ArgumentNullException("buffer");

      int numberOfBytesWritten;
      unsafe
      {
        void* pbuffer = (byte*)buffer + offset;
        {
          if (!WinApi.WriteFile(_handle, (IntPtr)pbuffer, count, out numberOfBytesWritten, IntPtr.Zero))
            throw new IOException("Unable to write to file", Marshal.GetLastWin32Error());
        }
        _position += numberOfBytesWritten;
      }
    }

    public override bool CanRead
    {
      get
      {
        return _canRead;
      }
    }

    public override bool CanSeek
    {
      get
      {
        return _canSeek;
      }
    }

    public override bool CanWrite
    {
      get
      {
        return _canWrite;
      }
    }

    public override long Length
    {
      get
      {
        long length;

        if (!WinApi.GetFileSizeEx(_handle, out length))
          throw new IOException("Unable to get file length", Marshal.GetLastWin32Error());

        return length;
      }
    }

    public override long Position
    {
      get
      {
        return _position;
      }
      set
      {
        Seek(value, SeekOrigin.Begin);
        _position = value;
      }
    }

    protected override void Dispose(bool disposing)
    {
      WinApi.CloseHandle(_handle);
      _handle = IntPtr.Zero;
      base.Dispose(disposing);
    }
  }

  static class OSFile
  {
    public static byte[] ReadAllBytes(string path)
    {
      using (var stream = new OSFileStream(path, FileMode.Open, FileAccess.Read))
      {
        var offset = 0;
        var length = stream.Length;

        if (length > int.MaxValue)
          throw new IOException("File too long");

        var count = (int)length;
        var buffer = new byte[count];

        while (count > 0)
        {
          int read = stream.Read(buffer, offset, count);
          if (read == 0)
            throw new EndOfStreamException();

          offset += read;
          count -= read;
        }

        return buffer;
      }
    }

    public static string ReadAllText(string path)
    {
      return ReadAllText(path, Encoding.UTF8);
    }

    public static string ReadAllText(string path, Encoding encoding)
    {
      using (var stream = new OSFileStream(path, FileMode.Open, FileAccess.Read))
      using (var reader = new StreamReader(stream, encoding, true, 0x400))
        return reader.ReadToEnd();
    }

#if NETFX_CORE
    public static DateTime GetLastWriteTime(string path)
    {
      var info = new WinApi.FILE_ATTRIBUTE_DATA();

      if (!WinApi.GetFileAttributesExW(path, 0, ref info))
        throw new IOException("Unable to get file into", Marshal.GetLastWin32Error());

      long result = (long)((ulong)info.ftLastWriteTimeHigh << 32 | (ulong)info.ftLastWriteTimeLow);
      return DateTime.FromFileTimeUtc(result).ToLocalTime();
    }

    public static bool Exists(string path)
    {
      var result = WinApi.GetFileAttributes(path);
      return (result != FileAttributes.Invalid) && ((result & FileAttributes.Directory) == 0);
    }

    public static void Delete(string path)
    {
      WinApi.DeleteFileW(path);
    }

    public static void Move(string sourcePath, string destPath)
    {
      WinApi.MoveFile(sourcePath, destPath);
    }
#endif
  }

#if NETFX_CORE
  static class OSDirectory
  {
    public static bool Exists(string path)
    {
      var result = WinApi.GetFileAttributes(path);

      if (result == FileAttributes.Invalid)
      {


        var e = Marshal.GetLastWin32Error();

      }

      return (result != FileAttributes.Invalid) && ((result & FileAttributes.Directory) != 0);
    }

    public static void CreateDirectory(string path)
    {
      for (int i = path.Length - 1; i >= 0; i--)
      {
        switch (path[i])
        {
          case '\\':
          case '/':
            if (Exists(path.Substring(0, i)))
            {
              CreateDirectoryFrom(i + 1, path);
              return;
            }
            break;
        }
      }

      CreateDirectoryFrom(0, path);
    }

    static void CreateDirectoryFrom(int start, string path)
    {
      for (int i = start, len = path.Length; i <= len; i++)
      {
        if (i == len)
          CheckDirectory(path);
        else
          switch (path[i])
          {
            case '\\':
            case '/':
              CheckDirectory(path.Substring(0, i));
              break;
          }
      }
    }

    static void CheckDirectory(string path)
    {
      if (!Exists(path) && !WinApi.CreateDirectoryW(path, IntPtr.Zero))
        throw new IOException("Unable to create directory", Marshal.GetLastWin32Error());
    }

    public static void Delete(string path, bool recursive)
    {
      if (recursive)
        throw new NotImplementedException();

      if (!WinApi.RemoveDirectoryW(path))
        throw new IOException("Unable to remove directory", Marshal.GetLastWin32Error());
    }
  }
#endif

  static class WinApi
  {
#if NETFX_CORE
    const string WinBaseDll = "api-ms-win-core-file-l2-1-0.dll";
    const string FileApiDll = "api-ms-win-core-file-l1-2-0.dll";
    const string HandleApiDll = "api-ms-win-core-handle-l1-1-0.dll";
#else
    const string WinBaseDll = "Kernel32.dll";
    const string FileApiDll = WinBaseDll;
    const string HandleApiDll = WinBaseDll;
#endif


#if NETFX_CORE
    [DllImport(FileApiDll, EntryPoint = "CreateFile2", SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern IntPtr CreateFile(string fileName, FileAccess desiredAccess, FileShare shareMode, FileMode mode, IntPtr extendedParameters);
#else
    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Ansi)]
    public static extern IntPtr CreateFile(string fileName, FileAccess desiredAccess, FileShare shareMode, IntPtr securityAttributes, FileMode mode, FileOptions flagsAndOptions, IntPtr templateFile);
#endif

    [DllImport(HandleApiDll, SetLastError = true)]
    public static extern bool CloseHandle(IntPtr handle);


    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool ReadFile(IntPtr fileHandle, IntPtr buffer, int numberOfBytesToRead, out int numberOfBytesRead, IntPtr overlapped);


    [DllImport(FileApiDll, SetLastError = true)]
    public static extern bool FlushFileBuffers(IntPtr hFile);


    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool WriteFile(IntPtr fileHandle, IntPtr buffer, int numberOfBytesToRead, out int numberOfBytesRead, IntPtr overlapped);


    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool SetFilePointerEx(IntPtr handle, long distanceToMove, out long distanceToMoveHigh, SeekOrigin seekOrigin);


    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool SetEndOfFile(IntPtr handle);

#if NETFX_CORE

    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool DeleteFileW(string path);


    [DllImport(WinBaseDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool MoveFileExW(string sourcePath, string targetPath, MOVE_FILE_FLAGS flags);

    public static bool MoveFile(string sourcePath, string targetPath)
    {
      return MoveFileExW(sourcePath, targetPath, MOVE_FILE_FLAGS.MOVEFILE_WRITE_THROUGH);
    }

    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool CreateDirectoryW(string path, IntPtr mustBeZero);

    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode)]
    public static extern bool RemoveDirectoryW(string path);

    [DllImport(FileApiDll, SetLastError = true, CharSet = CharSet.Unicode, BestFitMapping = false, ExactSpelling = false)]
    public static extern bool GetFileAttributesExW(string name, int fileInfoLevel, ref FILE_ATTRIBUTE_DATA lpFileInformation);

    public static FileAttributes GetFileAttributes(string path)
    {
      var data = new FILE_ATTRIBUTE_DATA();
      return GetFileAttributesExW(path, 0, ref data) ? (FileAttributes)data.fileAttributes : FileAttributes.Invalid;
    }

    public enum MOVE_FILE_FLAGS : int
    {
      /// <summary>
      /// If the file is to be moved to a different volume, the function simulates the move by using the CopyFile and DeleteFile functions.
      /// If the file is successfully copied to a different volume and the original file is unable to be deleted, the function succeeds leaving the source file intact.
      /// This value cannot be used with MOVEFILE_DELAY_UNTIL_REBOOT.
      /// </summary>
      MOVEFILE_COPY_ALLOWED = 0x2,

      /// <summary>
      /// Reserved for future use.
      /// </summary>
      MOVEFILE_CREATE_HARDLINK = 0x10,

      /// <summary>
      /// The system does not move the file until the operating system is restarted. The system moves the file immediately after AUTOCHK is executed, but before creating any paging files. Consequently, this parameter enables the function to delete paging files from previous startups.
      /// This value can be used only if the process is in the context of a user who belongs to the administrators group or the LocalSystem account. 
      /// This value cannot be used with MOVEFILE_COPY_ALLOWED.
      /// Windows Server 2003 and Windows XP:  For information about special situations where this functionality can fail, and a suggested workaround solution, see Files are not exchanged when Windows Server 2003 restarts if you use the MoveFileEx function to schedule a replacement for some files in the Help and Support Knowledge Base.
      /// </summary>
      MOVEFILE_DELAY_UNTIL_REBOOT = 0x4,

      /// <summary>
      /// The function fails if the source file is a link source, but the file cannot be tracked after the move. This situation can occur if the destination is a volume formatted with the FAT file system.
      /// </summary>
      MOVEFILE_FAIL_IF_NOT_TRACKABLE = 0x20,

      /// <summary>
      /// If a file named lpNewFileName exists, the function replaces its contents with the contents of the lpExistingFileName file, provided that security requirements regarding access control lists (ACLs) are met. For more information, see the Remarks section of this topic.
      /// This value cannot be used if lpNewFileName or lpExistingFileName names a directory.
      /// </summary>
      MOVEFILE_REPLACE_EXISTING = 0x1,

      /// <summary>
      /// The function does not return until the file is actually moved on the disk.
      /// Setting this value guarantees that a move performed as a copy and delete operation is flushed to disk before the function returns. The flush occurs at the end of the copy operation.
      /// This value has no effect if MOVEFILE_DELAY_UNTIL_REBOOT is set.
      /// </summary>
      MOVEFILE_WRITE_THROUGH = 0x8
    }

    enum FILE_INFO_BY_HANDLE_CLASS : int
    {
      FileBasicInfo = 0,
      FileStandardInfo = 1,
      FileNameInfo = 2,
      FileRenameInfo = 3,
      FileDispositionInfo = 4,
      FileAllocationInfo = 5,
      FileEndOfFileInfo = 6,
      FileStreamInfo = 7,
      FileCompressionInfo = 8,
      FileAttributeTagInfo = 9,
      FileIdBothDirectoryInfo = 10, // 0xA
      FileIdBothDirectoryRestartInfo = 11, // 0xB
      FileIoPriorityHintInfo = 12, // 0xC
      FileRemoteProtocolInfo = 13, // 0xD
      FileFullDirectoryInfo = 14, // 0xE
      FileFullDirectoryRestartInfo = 15, // 0xF
      FileStorageInfo = 16, // 0x10
      FileAlignmentInfo = 17, // 0x11
      MaximumFileInfoByHandlesClass
    };

    public struct FILE_ATTRIBUTE_DATA
    {
      public int fileAttributes;
      public uint ftCreationTimeLow;
      public uint ftCreationTimeHigh;
      public uint ftLastAccessTimeLow;
      public uint ftLastAccessTimeHigh;
      public uint ftLastWriteTimeLow;
      public uint ftLastWriteTimeHigh;
      public int fileSizeHigh;
      public int fileSizeLow;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct FILE_STANDARD_INFO
    {
      public long AllocationSize;
      public long EndOfFile;
      public int NumberOfLinks;
      public int DeletePending;
      public int Directory;
    };

    [DllImport(WinBaseDll, SetLastError = true, CharSet = CharSet.Unicode)]
    static extern bool GetFileInformationByHandleEx(IntPtr handle, FILE_INFO_BY_HANDLE_CLASS infoClass, IntPtr info, int size);

    public static bool GetFileSizeEx(IntPtr handle, out long fileSize)
    {
      FILE_STANDARD_INFO info;
      unsafe
      {
#if NETFX_CORE
        var infoSize = Marshal.SizeOf<FILE_STANDARD_INFO>();
#else
        var infoSize = Marshal.SizeOf(typeof(FILE_STANDARD_INFO));
#endif
        var result = GetFileInformationByHandleEx(handle, FILE_INFO_BY_HANDLE_CLASS.FileStandardInfo, new IntPtr(&info), infoSize);
        fileSize = info.EndOfFile;
        return result;
      }
    }

#else
    [DllImport(FileApiDll, EntryPoint = "GetFileSizeEx", SetLastError = true, CharSet = CharSet.Ansi)]
    public static extern bool GetFileSizeEx(IntPtr handle, out long fileSize);
#endif
  }
}
#endif
#endif