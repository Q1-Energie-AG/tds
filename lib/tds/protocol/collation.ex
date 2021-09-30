defmodule Tds.Protocol.Collation do
  import Tds.Protocol.Grammar

  @moduledoc """
  The collation rule is used to specify collation information for character data
  or metadata describing character data.

  This is typically specified as part of the LOGIN7 message or part of a column
  definition in server results containing character data.

  For more information about column definition, see
  COLMETADATA in MS-TDS.pdf.
  """

  @windows_437 "WINDOWS-437"
  @windows_850 "WINDOWS-850"
  @windows_874 "WINDOWS-874"
  @windows_932 "WINDOWS-932"
  @windows_936 "WINDOWS-936"
  @windows_949 "WINDOWS-949"
  @windows_950 "WINDOWS-950"
  @windows_1250 "WINDOWS-1250"
  @windows_1251 "WINDOWS-1251"
  @windows_1252 "WINDOWS-1252"
  @windows_1253 "WINDOWS-1253"
  @windows_1254 "WINDOWS-1254"
  @windows_1255 "WINDOWS-1255"
  @windows_1256 "WINDOWS-1256"
  @windows_1257 "WINDOWS-1257"
  @windows_1258 "WINDOWS-1258"
  @windows_utf8 "WINDOWS-UTF8"

  defstruct codepage: @windows_1252,
            lcid: nil,
            sort_id: nil,
            col_flags: nil,
            version: nil

  @type t :: %__MODULE__{
          codepage: String.t() | :RAW,
          lcid: nil | non_neg_integer,
          sort_id: non_neg_integer,
          col_flags: non_neg_integer,
          version: non_neg_integer
        }
  @typedoc """
  Value representing how much bytes is read from binary
  """
  @type bute_len :: non_neg_integer

  @spec encode(t) :: {:ok, <<_::40>>}
  def encode(%{codepage: :RAW}), do: {:ok, <<0x0::byte(5)>>}

  @spec decode(binary) ::
          {:ok, t}
          | {:error, :more}
          | {:error, any}
  def decode(<<0x0::byte(5)>>) do
    {:ok, struct!(__MODULE__, codepage: :RAW)}
  end

  def decode(<<
        lcid::bit(20),
        col_flags::bit(8),
        version::bit(4),
        sort_id::byte()
      >>) do
    codepage =
      decode_sortid(sort_id) ||
        decode_lcid(lcid) ||
        @windows_1252

    {:ok,
     struct!(__MODULE__,
       codepage: codepage,
       lcid: lcid,
       sort_id: sort_id,
       version: version,
       col_flags: col_flags
     )}
  end

  def decode(_), do: raise(Tds.Error, "Unrecognized collation")

  defp decode_sortid(sortid) do
    case sortid do
      0x1E -> @windows_437
      0x1F -> @windows_437
      0x20 -> @windows_437
      0x21 -> @windows_437
      0x22 -> @windows_437
      0x28 -> @windows_850
      0x29 -> @windows_850
      0x2A -> @windows_850
      0x2B -> @windows_850
      0x2C -> @windows_850
      0x31 -> @windows_850
      0x33 -> @windows_1252
      0x34 -> @windows_1252
      0x35 -> @windows_1252
      0x36 -> @windows_1252
      0x37 -> @windows_850
      0x38 -> @windows_850
      0x39 -> @windows_850
      0x3A -> @windows_850
      0x3B -> @windows_850
      0x3C -> @windows_850
      0x3D -> @windows_850
      0x50 -> @windows_1250
      0x51 -> @windows_1250
      0x52 -> @windows_1250
      0x53 -> @windows_1250
      0x54 -> @windows_1250
      0x55 -> @windows_1250
      0x56 -> @windows_1250
      0x57 -> @windows_1250
      0x58 -> @windows_1250
      0x59 -> @windows_1250
      0x5A -> @windows_1250
      0x5B -> @windows_1250
      0x5C -> @windows_1250
      0x5D -> @windows_1250
      0x5E -> @windows_1250
      0x5F -> @windows_1250
      0x60 -> @windows_1250
      0x68 -> @windows_1251
      0x69 -> @windows_1251
      0x6A -> @windows_1251
      0x6B -> @windows_1251
      0x6C -> @windows_1251
      0x70 -> @windows_1253
      0x71 -> @windows_1253
      0x72 -> @windows_1253
      0x78 -> @windows_1253
      0x79 -> @windows_1253
      0x7A -> @windows_1253
      0x7C -> @windows_1253
      0x80 -> @windows_1254
      0x81 -> @windows_1254
      0x82 -> @windows_1254
      0x88 -> @windows_1255
      0x89 -> @windows_1255
      0x8A -> @windows_1255
      0x90 -> @windows_1256
      0x91 -> @windows_1256
      0x92 -> @windows_1256
      0x98 -> @windows_1257
      0x99 -> @windows_1257
      0x9A -> @windows_1257
      0x9B -> @windows_1257
      0x9C -> @windows_1257
      0x9D -> @windows_1257
      0x9E -> @windows_1257
      0x9F -> @windows_1257
      0xA0 -> @windows_1257
      0xB7 -> @windows_1252
      0xB8 -> @windows_1252
      0xB9 -> @windows_1252
      0xBA -> @windows_1252
      # Don't use sort_id it is not SQL collation
      _ -> nil
    end
  end

  def decode_lcid(lcid) do
    case lcid do
      0x00436 -> @windows_1252
      0x00401 -> @windows_1256
      0x00801 -> @windows_1256
      0x00C01 -> @windows_1256
      0x01001 -> @windows_1256
      0x01401 -> @windows_1256
      0x01801 -> @windows_1256
      0x01C01 -> @windows_1256
      0x02001 -> @windows_1256
      0x02401 -> @windows_1256
      0x02801 -> @windows_1256
      0x02C01 -> @windows_1256
      0x03001 -> @windows_1256
      0x03401 -> @windows_1256
      0x03801 -> @windows_1256
      0x03C01 -> @windows_1256
      0x04001 -> @windows_1256
      0x0042D -> @windows_1252
      0x00423 -> @windows_1251
      0x00402 -> @windows_1251
      0x00403 -> @windows_1252
      0x30404 -> @windows_950
      0x00404 -> @windows_950
      0x00804 -> @windows_936
      0x20804 -> @windows_936
      0x01004 -> @windows_936
      0x0041A -> @windows_1250
      0x00405 -> @windows_1250
      0x00406 -> @windows_1252
      0x00413 -> @windows_1252
      0x00813 -> @windows_1252
      0x00409 -> @windows_1252
      0x00809 -> @windows_1252
      0x01009 -> @windows_1252
      0x01409 -> @windows_1252
      0x00C09 -> @windows_1252
      0x01809 -> @windows_1252
      0x01C09 -> @windows_1252
      0x02409 -> @windows_1252
      0x02009 -> @windows_1252
      0x00425 -> @windows_1257
      0x00438 -> @windows_1252
      0x00429 -> @windows_1256
      0x0040B -> @windows_1252
      0x0040C -> @windows_1252
      0x0080C -> @windows_1252
      0x0100C -> @windows_1252
      0x00C0C -> @windows_1252
      0x0140C -> @windows_1252
      0x10437 -> @windows_1252
      0x10407 -> @windows_1252
      0x00407 -> @windows_1252
      0x00807 -> @windows_1252
      0x00C07 -> @windows_1252
      0x01007 -> @windows_1252
      0x01407 -> @windows_1252
      0x00408 -> @windows_1253
      0x0040D -> @windows_1255
      0x00439 -> @windows_utf8
      0x0040E -> @windows_1250
      0x0104E -> @windows_1250
      0x0040F -> @windows_1252
      0x00421 -> @windows_1252
      0x00410 -> @windows_1252
      0x00810 -> @windows_1252
      0x00411 -> @windows_932
      0x10411 -> @windows_932
      0x00412 -> @windows_949
      0x00426 -> @windows_1257
      0x00427 -> @windows_1257
      0x00827 -> @windows_1257
      0x0041C -> @windows_1251
      0x00414 -> @windows_1252
      0x00814 -> @windows_1252
      0x00415 -> @windows_1250
      0x00816 -> @windows_1252
      0x00416 -> @windows_1252
      0x00418 -> @windows_1250
      0x00419 -> @windows_1251
      0x0081A -> @windows_1251
      0x00C1A -> @windows_1251
      0x0041B -> @windows_1250
      0x00424 -> @windows_1250
      0x0080A -> @windows_1252
      0x0040A -> @windows_1252
      0x00C0A -> @windows_1252
      0x0100A -> @windows_1252
      0x0140A -> @windows_1252
      0x0180A -> @windows_1252
      0x01C0A -> @windows_1252
      0x0200A -> @windows_1252
      0x0240A -> @windows_1252
      0x0280A -> @windows_1252
      0x02C0A -> @windows_1252
      0x0300A -> @windows_1252
      0x0340A -> @windows_1252
      0x0380A -> @windows_1252
      0x03C0A -> @windows_1252
      0x0400A -> @windows_1252
      0x0041D -> @windows_1252
      0x0041E -> @windows_874
      0x0041F -> @windows_1254
      0x00422 -> @windows_1251
      0x00420 -> @windows_1256
      0x0042A -> @windows_1258
      _ -> nil
    end
  end
end
