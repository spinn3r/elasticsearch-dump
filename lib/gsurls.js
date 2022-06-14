const gsurls = module.exports = {
    fromUrl: function(url) {
        if (url.startsWith("gs://")) {
            url = url.substring(5)
            const idx = url.indexOf("/")
            if (idx > 0) {
                return {
                    Bucket: url.slice(0, idx),
                    Key: url.slice(idx+1)
                };
            }
        }
        return {};
    },
    valid: function(url) {
        const params = fromUrl(url);
        return params.Bucket && params.Key;
    }
};