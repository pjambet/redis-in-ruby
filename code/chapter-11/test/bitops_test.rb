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

    it 'returns ...'
  end

  describe 'SETBIT' do
    it 'handles and unexpected number of arguments'
  end

  describe 'BITOP' do
    it 'handles and unexpected number of arguments'
  end

  describe 'BICOUNT' do
    it 'handles and unexpected number of arguments'
  end

  describe 'BITPOS' do
    it 'handles and unexpected number of arguments'
  end

  describe 'BITFIELD' do
    it 'handles and unexpected number of arguments'
  end
end
