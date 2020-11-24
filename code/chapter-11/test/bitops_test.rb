#!/usr/bin/env ruby
require_relative './test_helper'

describe 'Bitops Commands' do
  describe 'GETBIT' do
    it 'handles and unexpected number of arguments' do
      assert_command_results [
        [ 'GETBIT', '-ERR wrong number of arguments for \'GETBIT\' command' ],
        [ 'GETBIT s', '-ERR wrong number of arguments for \'GETBIT\' command' ],
      ]
    end

    it 'validates that offset is a positive integer' do
      assert_command_results [
        [ 'GETBIT s a', '-ERR bit offset is not an integer or out of range' ],
        [ 'GETBIT s -1', '-ERR bit offset is not an integer or out of range' ],
      ]
    end

    it 'returns an error if key is not a string' do
      assert_command_results [
        [ 'HSET not-a-string a b', ':1' ],
        [ 'GETBIT not-a-string a', '-WRONGTYPE Operation against a key holding the wrong kind of value' ],
      ]
    end

    it 'returns the bit at offset' do
      assert_command_results [
        [ 'SET s abc', '+OK' ],
        [ 'GETBIT s 0', ':0' ],
        [ 'GETBIT s 1', ':1' ],
        [ 'GETBIT s 2', ':1' ],
        [ 'GETBIT s 3', ':0' ],
      ]
    end
  end

  describe 'SETBIT' do
    it 'handles and unexpected number of arguments'
    it 'returns an error if key is not a string'
  end

  describe 'BITOP' do
    it 'handles and unexpected number of arguments'
    it 'returns an error if key is not a string'
  end

  describe 'BICOUNT' do
    it 'handles and unexpected number of arguments'
    it 'returns an error if key is not a string'
  end

  describe 'BITPOS' do
    it 'handles and unexpected number of arguments'
    it 'returns an error if key is not a string'
  end

  describe 'BITFIELD' do
    it 'handles and unexpected number of arguments'
    it 'returns an error if key is not a string'
  end
end
