Stream :notam_dev, nats_url: 'nats://localhost:4222', subjects: 'aftn.notam.*' do
  max_age '72h'
  max_message -1
end
