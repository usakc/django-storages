from storages.backends import azure_storage
from django.test import TestCase
try:
    from unittest import mock
except ImportError:  # Python 3.2 and below
    import mock
import datetime
from django.utils import timezone
from django.core.files.base import ContentFile
from azure.storage.blob import BlobProperties, Blob, BlobBlock
from django.utils.encoding import force_bytes


class AzureStorageTest(TestCase):

    def setUp(self, *args):
        self.storage = azure_storage.AzureStorage()
        self.storage._connection = mock.MagicMock()
        self.container_name = 'test'
        self.filename = 'test_file.txt'
        self.storage.azure_container = self.container_name

    def test_blob_exists(self):
        self.storage.connection.exists.return_value = True
        blob_name = "blob"
        exists = self.storage.exists(blob_name)
        self.assertTrue(exists)
        self.storage.connection.exists.assert_called_once_with(
            self.container_name, blob_name)

    def test_blob_doesnt_exists(self):
        self.storage.connection.exists.return_value = False
        blob_name = "blob"
        exists = self.storage.exists(blob_name)
        self.assertFalse(exists)
        self.storage.connection.exists.assert_called_once_with(
            self.container_name, blob_name)

    def test_blob_open_read(self):
        mocked_binary = b"mocked test"
        blob_name = "blob_name"
        sent_kwargs = {}

        def mocked_stream(*args, **kwargs):
            stream = kwargs['stream']
            stream.write(mocked_binary)
            sent_kwargs.update(kwargs)
            assert kwargs['max_connections'] == 1

        self.storage.connection.get_blob_to_stream.side_effect = mocked_stream
        with self.storage.open(blob_name, "rb") as f:
            content = f.read()
        self.assertEqual(mocked_binary, content)
        # I am doing this trick here to validate that the method was called, I couldn't use it with
        # the known parameter since a stream is an internal object that I don't
        # have access to
        self.storage.connection.get_blob_to_stream.assert_called_once_with(
            **sent_kwargs)

    def test_blob_open_text_write(self):
        mocked_text = "written text"

        with self.storage.open("name", "w") as f:
            f.write(mocked_text)
        self.storage.connection._put_blob.assert_called_once_with(self.container_name, "name", None)
        self.storage.connection.put_block.assert_called_once_with(self.container_name,
                                                                  "name", force_bytes(mocked_text),
                                                                  'MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwbmFtZTE%3D')
        put_block_list_call_list = self.storage.connection.put_block_list.call_args_list
        self.assertEqual(1, len(put_block_list_call_list))
        put_block_args = put_block_list_call_list[0]
        self.assertEqual(self.container_name, put_block_args[0][0])
        self.assertEqual("name", put_block_args[0][1])
        self.assertEqual(1, len(put_block_args[0][2]))
        self.assertIsInstance(put_block_args[0][2][0], BlobBlock)

    def test_blob_open_text_write_3_times(self):
        content1 = "content1"
        content2 = "content2"
        content3 = "content3"

        with self.storage.open("name", "w") as f:
            f.write(content1)
            f.write(content2)
            f.write(content3)
        self.storage.connection._put_blob.assert_called_once_with(self.container_name, "name", None)
        self.storage.connection.put_block.assert_called_once_with(self.container_name,
                                                                  "name", force_bytes(content1+content2+content3),
                                                                  'MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwbmFtZTE%3D')
        put_block_list_call_list = self.storage.connection.put_block_list.call_args_list
        self.assertEqual(1, len(put_block_list_call_list))
        put_block_args = put_block_list_call_list[0]
        self.assertEqual(self.container_name, put_block_args[0][0])
        self.assertEqual("name", put_block_args[0][1])
        self.assertEqual(1, len(put_block_args[0][2]))
        self.assertIsInstance(put_block_args[0][2][0], BlobBlock)

    def test_blob_open_text_write_3_times_small_buffer_size(self):
        contents = ["cont", "content2", "content3"]
        self.storage.buffer_size = 2

        with self.storage.open(self.filename, "w") as f:
            for content in contents:
                f.write(content)
        self.storage.connection._put_blob.assert_called_once_with(
            self.container_name, self.filename, None)
        put_block_args_list = self.storage.connection.put_block.call_args_list
        self.assertEqual(11, len(put_block_args_list))
        actual_content = bytes()
        for idx, args in enumerate(put_block_args_list):
            self.assertEqual(self.container_name, args[0][0])
            self.assertEqual(self.filename, args[0][1])
            self.assertLessEqual(len(args[0][2]), self.storage.buffer_size)
            actual_content = actual_content + (args[0][2])
        self.assertEqual(force_bytes("".join(contents)), actual_content)
        put_block_list_call_list = self.storage.connection.put_block_list.call_args_list
        self.assertEqual(1, len(put_block_list_call_list))
        put_block_args = put_block_list_call_list[0]
        self.assertEqual(self.container_name, put_block_args[0][0])
        self.assertEqual(self.filename, put_block_args[0][1])
        self.assertEqual(11, len(put_block_args[0][2]))
        for blob_block in put_block_args[0][2]:
            self.assertIsInstance(blob_block, BlobBlock)

    def test_delete_blob(self):
        self.storage.delete(self.filename)
        self.storage.connection.delete_blob.assert_called_once_with(container_name=self.container_name,
                                                                    blob_name=self.filename)

    def test_size_of_file(self):
        props = BlobProperties()
        props.content_length = 12
        self.storage.connection.get_blob_properties.return_value = Blob(
            props=props)
        size = self.storage.size(self.filename)
        self.assertEqual(12, size)

    def test_get_modified_time(self):
        naive_date = datetime.datetime(2017, 1, 2, 3, 4, 5, 678)
        aware_date = timezone.make_aware(naive_date, timezone.utc)

        props = BlobProperties()
        props.last_modified = aware_date
        self.storage.connection.get_blob_properties.return_value = Blob(
            props=props)

        with self.settings(TIME_ZONE='America/Montreal', USE_TZ=False):
            mt = self.storage.get_modified_time(self.filename)
            self.assertTrue(timezone.is_naive(mt))
            naive_date_montreal = timezone.make_naive(aware_date)
            self.assertEqual(mt, naive_date_montreal)
            self.storage.connection.get_blob_properties.assert_called_with(
                self.container_name, self.filename)

        with self.settings(TIME_ZONE='America/Montreal', USE_TZ=True):
            mt = self.storage.get_modified_time(self.filename)
            self.assertTrue(timezone.is_aware(mt))
            self.assertEqual(mt, aware_date)
            self.storage.connection.get_blob_properties.assert_called_with(
                self.container_name, self.filename)

    def test_url_blob(self):
        sas_token = "token"
        url = "url"
        blob = "blob"
        self.storage.connection.generate_blob_shared_access_signature.return_value = sas_token
        self.storage.connection.make_blob_url.return_value = url
        actual_url = self.storage.url(blob)
        self.assertEqual(url, actual_url)
        self.storage.connection.generate_blob_shared_access_signature.assert_not_called()
        self.storage.connection.make_blob_url.assert_called_once_with(blob_name=blob,
                                                                      container_name=self.container_name)

    def test_url_blob_with_expiry(self):
        sas_token = "token"
        url = "url"
        blob = "blob"
        self.storage.connection.generate_blob_shared_access_signature.return_value = sas_token
        self.storage.connection.make_blob_url.return_value = url
        self.storage._expire_at = mock.MagicMock(
            return_value=("now", 'expires_at'))
        actual_url = self.storage.url(blob, expire=30)
        self.assertEqual(url, actual_url)
        self.storage.connection.generate_blob_shared_access_signature.assert_called_once_with(self.container_name,
                                                                                              blob,
                                                                                              'r',
                                                                                              expiry='expires_at')
        self.storage.connection.make_blob_url.assert_called_once_with(blob_name=blob,
                                                                      container_name=self.container_name,
                                                                      sas_token=sas_token)

    def test_expires_at(self):
        expected_now = datetime.datetime.utcnow()
        now, now_plus_delta = self.storage._expire_at(expire=30)
        expected_now_plus_delta = now + datetime.timedelta(seconds=30)
        expected_now_plus_delta = expected_now_plus_delta.replace(
            microsecond=0).isoformat() + 'Z'
        self.assertEqual(expected_now_plus_delta, now_plus_delta)
        self.assertLess(now - expected_now, datetime.timedelta(seconds=1))

    def test_save(self):
        sent_kwargs = {}
        f = ContentFile("content")

        def validate_create_blob_from_stream(*args, **kwargs):
            sent_kwargs.update(kwargs)
            content_settings = kwargs['content_settings']
            assert content_settings.content_type == 'text/plain'
            content = kwargs['stream']
            assert content == f

        self.storage.connection.create_blob_from_stream.side_effect = validate_create_blob_from_stream
        self.storage._save("bla.txt", f)
        self.storage.connection.create_blob_from_stream.assert_called_once_with(
            **sent_kwargs)
