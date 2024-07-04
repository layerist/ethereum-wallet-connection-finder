# Ethereum Wallet Connection Finder

This Python script searches for a connection between two Ethereum wallet addresses using the Etherscan API. It logs the process and results to both the console and a file.

## Features

- Retrieve transactions of Ethereum addresses.
- Recursively search for connections between two addresses.
- Log the process and results to the console and a file.

## Requirements

- Python 3.6+
- Requests library

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/layerist/ethereum-wallet-connection-finder.git
    cd ethereum-wallet-connection-finder
    ```

2. Install the required Python package:

    ```sh
    pip install requests
    ```

## Usage

1. Get an Etherscan API key by registering on the [Etherscan website](https://etherscan.io/register).

2. Replace the placeholder `YOUR_API_KEY` in the script with your actual Etherscan API key.

3. Replace `0xAddress1` and `0xAddress2` in the script with the Ethereum addresses you want to check.

4. Run the script:

    ```sh
    python find_connection.py
    ```

### Example

Here's an example of how to set up the script:

```python
if __name__ == "__main__":
    # Replace these with the Ethereum addresses you want to check
    address1 = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
    address2 = "0x53d284357ec70ce289d6d64134dfac8e511c8a3d"
    main(address1, address2)
```

## Output

The script will print the progress to the console and save the log to `connection_log.txt`.

### Sample Console Output

```
Checking connection between 0x742d35Cc6634C0532925a3b844Bc454e4438f44e and 0x53d284357ec70ce289d6d64134dfac8e511c8a3d
Checking transactions of 0x742d35Cc6634C0532925a3b844Bc454e4438f44e at depth 1
Found 10 transactions for 0x742d35Cc6634C0532925a3b844Bc454e4438f44e
Checking transaction 0x123... from 0x742d35Cc6634C0532925a3b844Bc454e4438f44e to 0x123...
...
No connection found.
```

### Sample Log File (`connection_log.txt`)

```
Checking connection between 0x742d35Cc6634C0532925a3b844Bc454e4438f44e and 0x53d284357ec70ce289d6d64134dfac8e511c8a3d
Checking transactions of 0x742d35Cc6634C0532925a3b844Bc454e4438f44e at depth 1
Found 10 transactions for 0x742d35Cc6634C0532925a3b844Bc454e4438f44e
Checking transaction 0x123... from 0x742d35Cc6634C0532925a3b844Bc454e4438f44e to 0x123...
...
No connection found.
```

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add some feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Create a new Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
