"""Tests for the Solace queue setup script."""

import pytest
from unittest.mock import MagicMock, patch

from scripts.setup_solace_queue import create_queue, add_subscription


class TestCreateQueue:
    """Tests for create_queue function."""

    @patch("scripts.setup_solace_queue.requests.post")
    def test_create_queue_success(self, mock_post):
        """Test successful queue creation."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        result = create_queue(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="test-queue",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is True
        mock_post.assert_called_once()
        
        # Verify URL
        call_args = mock_post.call_args
        assert "http://localhost:8081/SEMP/v2/config/msgVpns/default/queues" == call_args[0][0]
        
        # Verify payload
        payload = call_args[1]["json"]
        assert payload["queueName"] == "test-queue"
        assert payload["accessType"] == "exclusive"
        assert payload["egressEnabled"] is True
        assert payload["ingressEnabled"] is True

    @patch("scripts.setup_solace_queue.requests.post")
    def test_create_queue_already_exists(self, mock_post):
        """Test queue creation when queue already exists."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "ALREADY_EXISTS"
        mock_post.return_value = mock_response
        
        result = create_queue(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="existing-queue",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is True

    @patch("scripts.setup_solace_queue.requests.post")
    def test_create_queue_failure(self, mock_post):
        """Test queue creation failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response
        
        result = create_queue(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="test-queue",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is False

    @patch("scripts.setup_solace_queue.requests.post")
    def test_create_queue_connection_error(self, mock_post):
        """Test queue creation with connection error."""
        import requests
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection refused")
        
        result = create_queue(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="test-queue",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is False


class TestAddSubscription:
    """Tests for add_subscription function."""

    @patch("scripts.setup_solace_queue.requests.post")
    def test_add_subscription_success(self, mock_post):
        """Test successful subscription creation."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        result = add_subscription(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="test-queue",
            topic="test/topic/>",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is True
        
        # Verify URL
        call_args = mock_post.call_args
        expected_url = "http://localhost:8081/SEMP/v2/config/msgVpns/default/queues/test-queue/subscriptions"
        assert expected_url == call_args[0][0]
        
        # Verify payload
        payload = call_args[1]["json"]
        assert payload["subscriptionTopic"] == "test/topic/>"

    @patch("scripts.setup_solace_queue.requests.post")
    def test_add_subscription_already_exists(self, mock_post):
        """Test subscription creation when it already exists."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "ALREADY_EXISTS"
        mock_post.return_value = mock_response
        
        result = add_subscription(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="test-queue",
            topic="existing/topic",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is True

    @patch("scripts.setup_solace_queue.requests.post")
    def test_add_subscription_failure(self, mock_post):
        """Test subscription creation failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Queue not found"
        mock_post.return_value = mock_response
        
        result = add_subscription(
            semp_url="http://localhost:8081",
            vpn="default",
            queue_name="nonexistent-queue",
            topic="test/topic",
            admin_user="admin",
            admin_password="admin",
        )
        
        assert result is False
